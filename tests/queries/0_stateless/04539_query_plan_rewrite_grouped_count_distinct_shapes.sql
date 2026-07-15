-- Tags: no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- no-parallel-replicas, no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET query_plan_rewrite_grouped_count_distinct = 1;

SET max_threads = 4;

DROP TABLE IF EXISTS t_cd_shapes;
CREATE TABLE t_cd_shapes (k UInt32, k2 UInt32, s String, lc LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number % 5, number % 3, concat('s', toString(number % 2503)), concat('lc', toString(number % 1499)),
          if(number % 5 = 0, 0, intHash64(number) % 2003)
FROM numbers(1000000);

SELECT 'String argument';
SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(s) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;

SELECT 'LowCardinality argument';
SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(lc) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;

SELECT 'expression argument';
SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(v * 2 + 1) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;

SELECT 'multiple group keys';
SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2 ORDER BY k, k2 LIMIT 6;
SELECT count() FROM (EXPLAIN SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2) WHERE explain LIKE '%Aggregating%';
SELECT k, k2, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k, k2 ORDER BY k, k2 LIMIT 6;

SELECT 'HAVING above the aggregation';
SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100 ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(v) AS u FROM t_cd_shapes GROUP BY k HAVING u > 100 ORDER BY k;

SELECT 'WHERE below the aggregation';
SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, uniqExact(v) AS u FROM t_cd_shapes WHERE k >= 3 GROUP BY k ORDER BY k;

SELECT 'count(DISTINCT ...) resolves to uniqExact and is rewritten';
SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';
SELECT k, count(DISTINCT v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;

SELECT 'a different function (uniq) is not rewritten';
SELECT k, uniq(v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniq(v) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';

SELECT 'a combinator (uniqExactIf) is not rewritten';
SELECT k, uniqExactIf(v, k >= 3) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExactIf(v, k >= 3) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';

SELECT 'multiple arguments are not rewritten';
SELECT k, uniqExact(s, v) AS u FROM t_cd_shapes GROUP BY k ORDER BY k;
SELECT count() FROM (EXPLAIN SELECT k, uniqExact(s, v) AS u FROM t_cd_shapes GROUP BY k) WHERE explain LIKE '%Aggregating%';

SELECT 'ROLLUP is not rewritten';
SELECT k % 3 AS a, uniqExact(v) AS u FROM t_cd_shapes GROUP BY a WITH ROLLUP ORDER BY a, u;
SELECT count() FROM (EXPLAIN SELECT k % 3 AS a, uniqExact(v) AS u FROM t_cd_shapes GROUP BY a WITH ROLLUP) WHERE explain LIKE '%Aggregating%';

DROP TABLE t_cd_shapes;
