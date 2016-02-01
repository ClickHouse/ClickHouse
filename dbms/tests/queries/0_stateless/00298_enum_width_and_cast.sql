DROP TABLE IF EXISTS test.enum;

CREATE TABLE test.enum (x Enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111), y UInt8) ENGINE = TinyLog;
INSERT INTO test.enum (y) VALUES (0);
SELECT * FROM test.enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO test.enum (x) VALUES ('\\');
SELECT * FROM test.enum ORDER BY x, y FORMAT PrettyCompact;
INSERT INTO test.enum (x) VALUES ('\t\\t');
SELECT * FROM test.enum ORDER BY x, y FORMAT PrettyCompact;
SELECT x, y, toInt8(x), toString(x) AS s, CAST(s AS Enum8('Hello' = -100, '\\' = 0, '\t\\t' = 111)) AS casted FROM test.enum ORDER BY x, y FORMAT PrettyCompact;

DROP TABLE test.enum;
