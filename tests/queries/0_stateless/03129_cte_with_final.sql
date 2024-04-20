CREATE OR REPLACE TABLE test
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO test Values (1, 'first', '2024-04-19 01:01:01');
INSERT INTO test Values (1, 'first', '2024-04-19 01:01:01');

EXPLAIN QUERY TREE passes=1
WITH merged_test AS(
	SELECT * FROM  test Final
)
SELECT * FROM  merged_test;

WITH merged_test AS(
	SELECT * FROM  test Final
)
SELECT * FROM  merged_test;
