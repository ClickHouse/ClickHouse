-- Random settings limits: optimize_use_projections=(1, None); optimize_use_projection_filtering=(1, None); optimize_aggregation_in_order=(0, 0)

-- A projection must not compute a function that can return another value on each evaluation, such as `rand`,
-- on its own rows: a row of an aggregate projection stands for a group of rows, and the projection index computes
-- the filter once more before the read computes it on the table rows.
-- https://github.com/ClickHouse/ClickHouse/issues/89615

SET min_table_rows_to_use_projection_index = 0;

DROP TABLE IF EXISTS t_agg;
DROP TABLE IF EXISTS t_idx;

-- 100 groups of 80 rows.
CREATE TABLE t_agg (id UInt64, k UInt64, PROJECTION p (SELECT k, count() GROUP BY k))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_agg SELECT number, number % 100 FROM numbers(8000);

SELECT '-- aggregate projection, a group loses all of its rows only by mistake';
SELECT 'rand in WHERE', count() FROM (SELECT k FROM t_agg WHERE rand() % 2 = 0 GROUP BY k);
SELECT 'rand in PREWHERE', count() FROM (SELECT k FROM t_agg PREWHERE rand() % 2 = 0 GROUP BY k);
SELECT 'rand in a lambda', count() FROM (SELECT k FROM t_agg WHERE arrayExists(x -> x = rand() % 2, [0]) GROUP BY k);
SELECT 'rand in DISTINCT', count() FROM (SELECT DISTINCT k FROM t_agg WHERE rand() % 2 = 0);
SELECT 'rand in GROUP BY', count() FROM (SELECT k, rand() % 2 AS r FROM t_agg GROUP BY k, r);
SELECT 'rand, projection used', count() > 0 FROM (EXPLAIN SELECT k, count() FROM t_agg WHERE rand() % 2 = 0 GROUP BY k SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%ReadFromMergeTree (p)%';
SELECT 'control: k < 50', count() FROM (SELECT k FROM t_agg WHERE k < 50 GROUP BY k);
SELECT 'control: projection used', count() > 0 FROM (EXPLAIN SELECT k, count() FROM t_agg WHERE k < 50 GROUP BY k SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%ReadFromMergeTree (p)%';

-- Every tenth row has b = 5.
CREATE TABLE t_idx (id UInt64, b UInt64, c UInt64, PROJECTION p INDEX b TYPE basic)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 64;
INSERT INTO t_idx SELECT number, (number * 7919) % 10, 1 FROM numbers(8000);

SELECT '-- projection index, rows out of 800 within 7 standard deviations of the expected count';
SELECT 'b = 5 AND rand', sum(c) BETWEEN 300 AND 500 FROM t_idx WHERE b = 5 AND rand() % 2 = 0;
SELECT 'rand in PREWHERE', sum(c) BETWEEN 300 AND 500 FROM t_idx PREWHERE b = 5 AND rand() % 2 = 0;
SELECT 'rand in a lambda', sum(c) BETWEEN 300 AND 500 FROM t_idx WHERE b = 5 AND arrayExists(x -> x = rand() % 2, [0]);
SELECT 'rand under OR', sum(c) BETWEEN 1100 AND 1300 FROM t_idx WHERE (b = 5 AND rand() % 2 = 0) OR b = 6;
SELECT 'b = 5 AND rand, projection used', count() > 0 FROM (EXPLAIN projections = 1 SELECT sum(c) FROM t_idx WHERE b = 5 AND rand() % 2 = 0 SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%will be applied during reading%';
SELECT 'control: b = 5', sum(c) FROM t_idx WHERE b = 5;

DROP TABLE t_agg;
DROP TABLE t_idx;
