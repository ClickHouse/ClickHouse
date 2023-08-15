DROP TABLE IF EXISTS aboba;
CREATE TABLE aboba (s String, ui UInt8 CODEC(GCD(1), LZ4)) ENGINE = Memory;
INSERT INTO aboba (*) VALUES ('Hello', 239), ('World', 0), ('Goodbye', 37);
SELECT ui FROM aboba;
