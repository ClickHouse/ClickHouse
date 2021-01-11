DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1 (s String) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;
CREATE TABLE test2 (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 1;

INSERT INTO test1 SELECT toString(number) FROM numbers(10000);
INSERT INTO test2 SELECT toString(number) FROM numbers(10000);

SELECT s FROM test1 WHERE toString(s) = '1234' SETTINGS max_rows_to_read = 2;
SELECT s FROM test2 WHERE toString(s) = '1234' SETTINGS max_rows_to_read = 2;

DROP TABLE test1;
DROP TABLE test2;
