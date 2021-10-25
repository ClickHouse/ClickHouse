SELECT DISTINCT a FROM remote('127.0.0.{1,2,3}', values('a UInt8, b UInt8', (1, 2), (1, 3))) GROUP BY a, b;
