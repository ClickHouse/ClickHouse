WITH toIPv4('127.0.0.10') AS ip
SELECT
    ip = 2130706442::UInt32,
    ip = 0::UInt32,
    ip < 2130706443::UInt32,
    ip > 2130706441::UInt32,
    ip <= 2130706442::UInt32,
    ip >= 2130706442::UInt32,
    ip != 2130706442::UInt32;
