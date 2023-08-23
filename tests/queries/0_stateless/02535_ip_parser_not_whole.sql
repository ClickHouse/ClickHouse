SELECT * FROM format(CSVWithNamesAndTypes, 'ip,port\nIPv6,UInt16\n::1,42\n');
SELECT * FROM format(TSVWithNamesAndTypes, 'ip\tport\nIPv6\tUInt16\n::1\t42\n');
SELECT * FROM format(JSONCompactEachRowWithNamesAndTypes, '["ip","port"]\n["IPv6","UInt16"]\n["::1",42]\n');
