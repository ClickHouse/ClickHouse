DROP TABLE IF EXISTS underlying_01795;
CREATE TABLE underlying_01795 (key UInt64) Engine=TinyLog();
INSERT INTO FUNCTION remote('127.1', currentDatabase(), underlying_01795) SELECT toUInt64(number) FROM system.numbers LIMIT 1;
SELECT * FROM underlying_01795 FORMAT Null;
DROP TABLE underlying_01795;
