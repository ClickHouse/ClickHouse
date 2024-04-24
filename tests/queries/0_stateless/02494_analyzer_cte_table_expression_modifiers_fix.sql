DROP TABLE IF EXISTS test;

CREATE TABLE test
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO test Values (1, 'first', '2024-04-19 01:01:01');
INSERT INTO test Values (1, 'first', '2024-04-19 01:01:01');

WITH merged_test AS(SELECT * FROM  test Final) SELECT * FROM  merged_test;
