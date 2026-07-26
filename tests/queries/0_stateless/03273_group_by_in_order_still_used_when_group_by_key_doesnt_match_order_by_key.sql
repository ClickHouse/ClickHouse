CREATE TABLE test
(
  a UInt64,
  b UInt64
)
ENGINE = MergeTree
ORDER BY (a, b);

INSERT INTO test SELECT number, number FROM numbers_mt(1e6);

set enable_analyzer = 1;

SELECT trimBoth(replaceRegexpAll(explain, '__table1.', ''))
FROM
(
  EXPLAIN actions = 1
  SELECT count(*)
  FROM test
  GROUP BY
      b,
      a
  SETTINGS optimize_aggregation_in_order = 1
)
WHERE explain LIKE '%Order%';
