SET enable_analyzer = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/45804

CREATE TABLE myRMT(
  key Int64,
  someCol String,
  ver DateTime
) ENGINE = ReplacingMergeTree(ver)
ORDER BY key as SELECT 1, 'test', '2020-01-01';

SELECT count(ver) FROM myRMT FINAL PREWHERE ver > '2000-01-01';

SELECT count() FROM myRMT FINAL PREWHERE ver > '2000-01-01';

DROP TABLE myRMT;
