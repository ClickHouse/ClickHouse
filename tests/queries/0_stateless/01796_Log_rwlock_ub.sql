DROP TABLE IF EXISTS underlying_01796;
CREATE TABLE underlying_01796 (key UInt64) Engine=Log();
INSERT INTO FUNCTION remote('127.1', currentDatabase(), underlying_01796) SELECT toUInt64(number) FROM system.numbers LIMIT 1;
SELECT * FROM underlying_01796 FORMAT Null;
DROP TABLE underlying_01796;
