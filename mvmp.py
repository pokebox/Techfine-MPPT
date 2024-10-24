#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import time
import serial
import traceback
import logging
import struct
from datetime import datetime
import requests

class mvmp:
    def __init__(self) -> None:
        self.serial = serial.Serial()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.timeoutSleep = 0.1    # 超时等待休眠时间
        self.timeoutCountMax = 10  # 超时等待次数

    def send_sensor_data(self, sensor_name: str, sensor_value, data: dict, sensor_type: str = "sensor"):
        url = f"https://<HASS_IP>:<HASS_PORT>/api/states/{sensor_type}.{sensor_name}"
        headers = {
            "Authorization": "Bearer <API_TOKEN>",
            "Content-Type": "application/json",
        }
        datas = {
            "state": sensor_value,
            "attributes": data
        }
        # 发送 POST 请求
        response = requests.post(url, headers=headers, json=datas)
        
        if response.status_code == 200:
            logger.debug(f"Successfully updated sensor: {sensor_value}")
        else:
            logger.warning(f"Failed to update sensor: {response.status_code} - {response.text}")

    def connect(self, port, baudrate=2400, timeout=1):
        self.serial = serial.Serial(port, baudrate, timeout=timeout)

    def waitData(self):
        '''
        等待数据
        '''
        timeoutCount = 0
        while not self.serial.in_waiting:
            time.sleep(self.timeoutSleep)
            timeoutCount += 1
            if timeoutCount > self.timeoutCountMax:
                self.logger.error("Write Register Timeout")
                return False
        try:
            retdata = self.serial.readall()
            self.logger.debug(str(retdata))
            return retdata
        except Exception as e:
            self.logger.error(e)
            return False

    def setSystemTime(self, dt: datetime):
        # 固定数据
        fixed_data = b'\x43\x54\x49\x4D\x0D\x52\x45\x54\x49\x4D\x45'
        
        # 时间数据
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        minute = dt.minute
        second = dt.second
        yearH = dt.year // 256
        yearL = dt.year % 256
        
        # 构造时间数据
        time_data = struct.pack('<BBBBBBB', yearH, yearL, month, day, hour, minute, second)
        
        # 合并固定数据和时间数据
        data_to_send = fixed_data + time_data
        
        # 发送数据
        self.serial.write(data_to_send)
        print(f"Sent: {data_to_send.hex()}")

    def getData(self):
        self.serial.write(b'CMHB\x06MPPTHB')
        time.sleep(0.1)
        return self.waitData()

    def parse_status_byte1(self, byte):
        return {
            'chargingTempLimit': 'ON' if (byte >> 3) & 0x01 == 1 else 'Off',
            'batteryVoltageFull': True if (byte >> 2) & 0x01 == 1 else False,
            'chargingStatus': ' Charging' if (byte >> 1) & 0x01 == 1 else 'Not Charging',
            'dcOutputStatus': 'ON' if (byte & 0x01) == 1 else 'Off'
        }

    def parse_status_byte2(self,byte):
        return {
            'alarmStatus': True if (byte >> 1) & 0x01 == 1 else False,  # 告警状态
            'faultStatus': True if byte & 0x01 == 1 else False          # 故障状态
        }

    def parse_warn_byte1(self, byte):
        return {
            'solarVoltageHigh': (byte >> 7) & 0x01,     # 光伏电压过高
            'batteryTempHigh': (byte >> 6) & 0x01,      # 电池温度过高
            'machineTempHigh': (byte >> 5) & 0x01,      # 机器温度过高
            'chipTempHigh': (byte >> 4) & 0x01,         # 芯片温度过高
            'storageDataError': (byte >> 3) & 0x01,     # 存储数据错误
            'storageOperationFail': (byte >> 2) & 0x01, # 存储操作失败
            'clockOscillatorFail': (byte >> 1) & 0x01,  # 时钟振荡器故障
            'externalOscillatorFail': byte & 0x01       # 外部振荡器故障
        }

    def parse_warn_byte2(self, byte):
        return {
            'batteryVoltageError': (byte >> 4) & 0x01,  # 电池电压判断错误
            'batteryTempHigh': (byte >> 3) & 0x01,      # 电池温度过高
            'loadOverAlarm': (byte >> 2) & 0x01,        # 负载过载告警
            'batteryVoltageLow': (byte >> 1) & 0x01,    # 电池电压低
            'batteryVoltageVeryLow': byte & 0x01        # 电池电压过低
        }

    def parse_fault_byte1(self, byte):
        return {
            'batteryTempLow': (byte >> 7) & 0x01,           # 电池温度过低
            'batteryTempSensorFault': (byte >> 6) & 0x01,   # 电池温度传感器故障
            'batteryVoltageHigh': (byte >> 5) & 0x01,       # 电池电压过高
            'machineTempSensorFault': (byte >> 4) & 0x01,   # 机器温度传感器故障
            'dcOutputFaultLock': (byte >> 3) & 0x01,        # 直流输出故障锁定
            'batteryTempHighFault': (byte >> 2) & 0x01,     # 电池温度过高故障
            'machineTempHighFault': (byte >> 1) & 0x01,     # 机器温度过高故障
            'chipTempHighFault': byte & 0x01                # 芯片温度过高故障
        }

    def parse_fault_byte2(self, byte):
        return {
            'batteryVoltageInputError': byte & 0x01     # 电池电压输入错误
        }

    # 解析数据并返回详细状态
    def parseData(self, data):
        if not data.startswith(b'MVMP'):
            self.logger.error("Invalid frame header")
            return None

        data_length = data[4]
        mppt_data = data[5:5+data_length-1]
        checksum = data[4+data_length]

        # Calculate checksum
        calc_checksum = sum(mppt_data) & 0xFF
        if calc_checksum != checksum:
            self.logger.error("Checksum mismatch")
            return None

        # Parse MPPT status data
        status = {
            'statusByte1': self.parse_status_byte1(mppt_data[0]),
            'statusByte2': self.parse_status_byte2(mppt_data[1]),
            # statusByte3': mppt_data[2],  # 保留字节
            'warnByte1': self.parse_warn_byte1(mppt_data[3]),
            'warnByte2': self.parse_warn_byte2(mppt_data[4]),
            # 'warnByte3': mppt_data[5],  # 保留字节
            'faultByte1': self.parse_fault_byte1(mppt_data[6]),
            'faultByte2': self.parse_fault_byte2(mppt_data[7]),
            'programVersionMajor': mppt_data[8],                    # 程序版本号(主版本号)
            'programVersionMinor': mppt_data[9],                    # 程序版本号(次版本号)
            'pvInputVoltage': struct.unpack('<H', mppt_data[10:12])[0], # 光伏输入电压
            'batteryCapacity': mppt_data[12],                           # 电池容量
            'batteryCells': mppt_data[13],                              # 电池节数
            'batteryVoltage': struct.unpack('<H', mppt_data[14:16])[0], # 电池电压
            'batteryType': mppt_data[16],                   # 电池类型
            'batteryTemperature': mppt_data[17],            # 电池温度
            'chargingCurrent': mppt_data[18],               # 充电电流
            'chipTemperature': mppt_data[19],               # 芯片温度
            'machineTemperature': mppt_data[20],            # 机器温度
            'loadPercentage': mppt_data[21],                # 负载百分比
            'dailyGeneratedEnergyInt': struct.unpack('<H', mppt_data[22:24])[0],    # 日发电量(整数部分)
            'dailyGeneratedEnergyDec': struct.unpack('<H', mppt_data[24:26])[0],    # 日发电量(小数部分)
            'machineType': mppt_data[26],           # 机器类型
            'totalGeneratedEnergyDec': mppt_data[27],                               # 总发电量(小数部分)
            'totalGeneratedEnergyInt': struct.unpack('<H', mppt_data[28:30])[0],    # 总发电量(整数部分)
            'systemRunTimeInt': struct.unpack('<H', mppt_data[30:32])[0],       # 系统运行时间(整数部分)
            'systemRunTimeDec': mppt_data[32],                          # 系统运行时间(小数部分)
            'systemStatusCode': mppt_data[33],
            'systemTimeYear': struct.unpack('<H', mppt_data[34:36])[0],
            'systemTimeMonth': mppt_data[36],
            'systemTimeDay': mppt_data[37],
            'systemTimeHour': mppt_data[38],
            'systemTimeMinute': mppt_data[39],
            'systemTimeSecond': mppt_data[40],
            'systemTimeWeekday': mppt_data[41],
            'storedBatteryType': mppt_data[42],
            'storedSystemVoltage': mppt_data[43],
            'storedChargingPercentage': mppt_data[44],
            'storedBatteryLowOutputVoltage': mppt_data[45],
            'storedBatteryStrongChargingVoltage': mppt_data[46],
            'storedBatteryFloatChargingVoltage': mppt_data[47],
            'storedDCOutputType': mppt_data[48],
            'storedDCTurnOnHour': mppt_data[49],
            'storedDCTurnOnMinute': mppt_data[50],
            'storedDCTurnOffHour': mppt_data[51],
            'storedDCTurnOffMinute': mppt_data[52],
            'storedDCOutputRecoveryVoltage': mppt_data[53],
            'chargingCurrentDecimal': mppt_data[54],
            'reserved': mppt_data[55]
        }

        statusDict = dict()
        statusDict.update(status['statusByte1'])
        statusDict.update(status['statusByte2'])
        warnDict = dict()
        warnDict.update(status['warnByte1'])
        warnDict.update(status['warnByte2'])
        faultDict = dict()
        faultDict.update(status['faultByte1'])
        faultDict.update(status['faultByte2'])
        # statusDict.update(warnDict)
        # statusDict.update(faultDict)
        dailyGeneratedEnergy = float(f"{status['dailyGeneratedEnergyInt']}.{status['dailyGeneratedEnergyDec']}")
        totalGeneratedEnergy = float(f"{status['totalGeneratedEnergyInt']}.{status['totalGeneratedEnergyDec']}")
        chargeCurrent = float(f"{status['chargingCurrent']}.{status['chargingCurrentDecimal']}")
        systemRunTime = float(f"{status['systemRunTimeInt']}.{status['systemRunTimeDec']}")
        systemTime = f"{status['systemTimeYear']:04d}-{status['systemTimeMonth']:02d}-{status['systemTimeDay']:02d} {status['systemTimeHour']:02d}:{status['systemTimeMinute']:02d}:{status['systemTimeSecond']:02d}"
        programVersion = f"{status['programVersionMajor']}.{status['programVersionMinor']}"
        outStatus = {
            "status": statusDict,
            "warn": warnDict,
            "fault": faultDict,
            "pvInputVoltage": status['pvInputVoltage'],
            "batteryVoltage": status['batteryVoltage']/10.0,
            "chargingCurrent": chargeCurrent,
            "chargingPower": round(status['batteryVoltage']/10.0*chargeCurrent, 3),
            "batteryCapacity": status['batteryCapacity'],
            "batteryCells": status['batteryCells'],
            "loadPercentage": status['loadPercentage'],
            "dailyGeneratedEnergy": dailyGeneratedEnergy,
            "totalGeneratedEnergy": totalGeneratedEnergy,
            "systemRunTime": systemRunTime,
            "systemTime": systemTime,
            "storedBatteryType": status['storedBatteryType'],
            "storedSystemVoltage": status['storedSystemVoltage']/10.0,
            "storedChargingPercentage": status['storedChargingPercentage'],
            "storedBatteryLowOutputVoltage": status['storedBatteryLowOutputVoltage']/10.0,
            "storedBatteryStrongChargingVoltage": status['storedBatteryStrongChargingVoltage']/10.0,
            "storedBatteryFloatChargingVoltage": status['storedBatteryFloatChargingVoltage']/10.0,
            "storedDCOutputType": status['storedDCOutputType'],
            "storedDCTurnOnTime": f"{status['storedDCTurnOnHour']:02d}:{status['storedDCTurnOnMinute']:02d}",
            "storedDCTurnOffTime": f"{status['storedDCTurnOffHour']:02d}:{status['storedDCTurnOffMinute']:02d}",
            "storedDCOutputRecoveryVoltage": status['storedDCOutputRecoveryVoltage']/10.0,
            "programVersion": programVersion,
            "machineType": status['machineType'],
            "machineTemperature": status['machineTemperature'],
            "chipTemperature": status['chipTemperature'],
            "batteryTemperature": None if status['batteryTemperature'] == 255 else status['batteryTemperature']
        }

        outCNStatus = {
            "状态": statusDict,
            "告警": warnDict,
            "故障": faultDict,
            "光伏输入电压": f"{outStatus['pvInputVoltage']}V",
            "电池电压": f"{outStatus['batteryVoltage']}V",
            "充电电流": f"{chargeCurrent}A",
            "计算充电功率": f"{(outStatus['chargingPower']):.2f}W",
            "电池容量": outStatus['batteryCapacity'],
            "电池节数": outStatus['batteryCells'],
            "负载百分比": f"{outStatus['loadPercentage']}%",
            "日发电量": f"{dailyGeneratedEnergy}kWh",
            "总发电量": f"{totalGeneratedEnergy}kWh",
            "系统运行时间": f"{systemRunTime}h",
            "系统时间": systemTime,
            "存储电池类型": outStatus['storedBatteryType'],
            "存储系统电压": f"{outStatus['storedSystemVoltage']}V",
            "存储充电百分比": f"{outStatus['storedChargingPercentage']}%",
            "存储电池低输出电压": f"{outStatus['storedBatteryLowOutputVoltage']}V",
            "存储电池强充电电压": f"{outStatus['storedBatteryStrongChargingVoltage']}V",
            "存储电池浮充电电压": f"{outStatus['storedBatteryFloatChargingVoltage']}V",
            "存储直流输出类型": outStatus['storedDCOutputType'],
            "存储直流输出启动时间": f"{outStatus['storedDCTurnOnTime']}",
            "存储直流输出关闭时间": f"{outStatus['storedDCTurnOffTime']}",
            "存储直流输出恢复电压": f"{outStatus['storedDCOutputRecoveryVoltage']}V",
            "程序版本": programVersion,
            "机器类型": outStatus['machineType'],
            "机器温度": f"{outStatus['machineTemperature']}°C",
            "芯片温度": f"{outStatus['chipTemperature']}°C",
            "电池温度": f"{outStatus['batteryTemperature']}°C"
        }
        self.send_sensor_data("mppt_pvInputVoltage", outStatus['pvInputVoltage'], {"device_class": "voltage", "unit_of_measurement": "V", "friendly_name": "光伏输入电压"})
        self.send_sensor_data("mppt_batteryVoltage", outStatus['batteryVoltage'], {"device_class": "voltage", "unit_of_measurement": "V", "friendly_name": "电池电压"})
        self.send_sensor_data("mppt_chargingCurrent", chargeCurrent, {"device_class": "current", "unit_of_measurement": "A", "friendly_name": "充电电流"})
        self.send_sensor_data("mppt_chargingPower", outStatus['chargingPower'], {"device_class": "power", "unit_of_measurement": "W", "friendly_name": "计算充电功率"})
        self.send_sensor_data("mppt_batteryCapacity", outStatus['batteryCapacity'], {"device_class": "battery", "unit_of_measurement": "Ah", "friendly_name": "电池容量"})
        self.send_sensor_data("mppt_batteryCells", outStatus['batteryCells'], {"device_class": "value", "unit_of_measurement": "cells", "friendly_name": "电池节数"})
        # self.send_sensor_data("mppt_loadPercentage", outStatus['loadPercentage'], {"device_class": "battery", "unit_of_measurement": "%", "friendly_name": "负载百分比"})
        self.send_sensor_data("mppt_dailyGeneratedEnergy", dailyGeneratedEnergy, {"device_class": "energy", "unit_of_measurement": "kWh", "friendly_name": "日发电量", "state_class": "total"})
        self.send_sensor_data("mppt_totalGeneratedEnergy", totalGeneratedEnergy, {"device_class": "energy", "unit_of_measurement": "kWh", "friendly_name": "总发电量", "state_class": "total_increasing"})
        self.send_sensor_data("mppt_systemRunTime", systemRunTime, {"device_class": "value", "unit_of_measurement": "h", "friendly_name": "系统运行时间"})
        self.send_sensor_data("mppt_programVersion", programVersion, {"device_class": "version", "unit_of_measurement": "", "friendly_name": "程序版本"})
        self.send_sensor_data("mppt_machineTemperature", outStatus['machineTemperature'], {"device_class": "temperature", "unit_of_measurement": "°C", "friendly_name": "机器温度"})
        if outStatus['batteryTemperature'] is not None:
            self.send_sensor_data("mppt_batteryTemperature", outStatus['batteryTemperature'], {"device_class": "temperature", "unit_of_measurement": "°C", "friendly_name": "电池温度"})

        return outCNStatus

if __name__ == '__main__':
    import sys
    import coloredlogs
    import json

    # 设置coloredlogs，仅用于控制台显示
    coloredlogs.install(level='INFO', fmt="%(asctime)s %(filename)s[%(lineno)d] %(levelname)s: %(message)s", milliseconds=True)

    # 创建一个日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 创建一个处理器，用于将日志消息写入文件
    file_handler = logging.FileHandler('my_log.log')
    file_handler.setLevel(logging.INFO)

    # 创建一个格式器，用于规范日志消息的输出
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # 为日志对象添加文件处理器
    #logger.addHandler(file_handler)
    if len(sys.argv) < 2:
        print("Usage: python mvmp.py COM1")
        sys.exit()
    port = sys.argv[1]
    m = mvmp()
    m.connect(port)
    if True:
        m.setSystemTime(datetime.now())
        print(f"Set system time to {datetime.now()}")
        time.sleep(2)
        data = m.getData()
        if data:
            status = m.parseData(data)
            if status:
                print(status)
    while True:
        data = m.getData()
        if data:
            status = m.parseData(data)
            if status:
                logger.info(f"{json.dumps(status, indent=4, ensure_ascii=False)},")
        # 如果是晚上就延长等待时间
        if datetime.now().hour >= 20 or datetime.now().hour < 6:
            time.sleep(30)
        else:
            time.sleep(1)
    
    m.serial.close()
