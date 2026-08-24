SELECT arrayJoin(['1.1.1.1', '255.255.255.255']) AS x, toIPv4(x) AS y, toUInt32(y) AS z FORMAT PrettyCompactNoEscapes;
