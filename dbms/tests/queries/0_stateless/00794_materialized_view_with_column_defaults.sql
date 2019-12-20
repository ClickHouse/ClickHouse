DROP TABLE IF EXISTS test1;

CREATE TABLE test1 (
  date Date,
  datetime DateTime,
  zoneId UInt64,
  test ALIAS zoneId == 1
) ENGINE = MergeTree(date, (date, zoneId), 8192);

CREATE MATERIALIZED VIEW test1_view
ENGINE = MergeTree(date, (date, zoneId), 8192)
AS SELECT
  date,
  zoneId,
  test
FROM test1;

INSERT INTO test1 VALUES ('2018-12-10', '2018-12-10 23:59:59', 1);

SELECT * from test1_view;

DROP TABLE test1_view;
DROP TABLE test1;
