DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO t Values (1, 'first', '2024-04-19 01:01:01');
INSERT INTO t Values (1, 'first', '2024-04-19 01:01:01');

SET enable_analyzer = 1;

EXPLAIN QUERY TREE passes=1
WITH merged_test AS(
	SELECT * FROM  t Final
)
SELECT * FROM  merged_test;

WITH merged_test AS(
	SELECT * FROM  t Final
)
SELECT * FROM  merged_test;

DROP TABLE t;
