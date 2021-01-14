DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8, y String, z Array(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1);

SELECT name, type, position FROM system.columns WHERE database = currentDatabase() AND table = 'test';
SELECT column, type, column_position FROM system.parts_columns WHERE database = currentDatabase() AND table = 'test';

DROP TABLE test;
