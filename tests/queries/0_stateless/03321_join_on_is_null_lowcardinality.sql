DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test2;

SET enable_analyzer = 1;

CREATE TABLE test ( `id` UInt32, `value` LowCardinality(Nullable(String)) )
ENGINE = MergeTree ORDER BY id;

CREATE TABLE test2 ( `id` UInt32, `value` LowCardinality(Nullable(String)) )
ENGINE = MergeTree ORDER BY id;

INSERT INTO test VALUES (123, NULL);
INSERT INTO test2 VALUES (123, NULL);

SELECT * FROM test
FULL JOIN test2
ON test.value IS NULL AND test2.value IS NULL OR test.value == test2.value
SETTINGS join_use_nulls = 1
;

SELECT * FROM test
FULL JOIN test2
ON test.value == test2.value
ORDER BY ALL
SETTINGS join_use_nulls = 1
;
