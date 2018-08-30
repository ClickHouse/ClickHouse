USE test;
DROP TABLE IF EXISTS where_qualified;
CREATE TABLE where_qualified(a UInt32, b UInt8) ENGINE = Memory;
INSERT INTO where_qualified VALUES(1, 1);
INSERT INTO where_qualified VALUES(2, 0);
SELECT a from where_qualified WHERE where_qualified.b;
DROP TABLE where_qualified;
