DROP TABLE IF EXISTS test;
CREATE TABLE test (x Enum('hello' = 1, 'world' = 2), y String) ENGINE = MergeTree PARTITION BY x ORDER BY y;
INSERT INTO test VALUES ('hello', 'test');

SELECT * FROM test;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 3);
INSERT INTO test VALUES ('goodbye', 'test');
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'test' = 3);
ALTER TABLE test MODIFY COLUMN x Enum('hello' = 1, 'world' = 2, 'goodbye' = 4); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test MODIFY COLUMN x Int8;
INSERT INTO test VALUES (111, 'abc');
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY x;
SELECT name, partition, partition_id FROM system.parts WHERE database = currentDatabase() AND table = 'test' AND active ORDER BY partition;

ALTER TABLE test MODIFY COLUMN x Enum8('' = 1); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Enum16('' = 1); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test MODIFY COLUMN x UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x String; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test MODIFY COLUMN x Nullable(Int64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

ALTER TABLE test RENAME COLUMN x TO z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test RENAME COLUMN y TO z; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN x; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE test DROP COLUMN y; -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE test;
