-- Tags: no-random-settings

-- Test: LIMIT push-down into aggregation-in-order
-- When GROUP BY key = ORDER BY key = table PK and LIMIT is present,
-- the aggregation should stop early after producing enough groups.

DROP TABLE IF EXISTS t_agg_in_order_limit;

CREATE TABLE t_agg_in_order_limit (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16;

-- Insert 1000 rows: 100 distinct keys (0..99), 10 rows each.
-- Use ORDER BY to ensure data is inserted in sorted order within single part.
INSERT INTO t_agg_in_order_limit SELECT number % 100 AS key, number AS value FROM numbers(1000) ORDER BY key;

-- Basic: GROUP BY pk ORDER BY pk LIMIT
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- With OFFSET
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 3 OFFSET 5
SETTINGS optimize_aggregation_in_order = 1;

-- Multiple aggregate functions
SELECT key, count(), sum(value) FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: HAVING present — should still return correct results
SELECT key, count() AS c FROM t_agg_in_order_limit GROUP BY key HAVING c > 5 ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: ORDER BY DESC — should still return correct results
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key DESC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: WITH TOTALS — should still return correct results
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key WITH TOTALS ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: ARRAY JOIN between aggregation and outer LIMIT changes row count;
-- the optimization must not push the limit through ExpressionStep that has ARRAY JOIN.
SELECT key, count() FROM (
    SELECT key FROM t_agg_in_order_limit GROUP BY key
) ARRAY JOIN [1, 2, 3] AS x
GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1;

-- Negative case: extremes are computed before LIMIT — pushing the limit past
-- ExtremesStep would give wrong min/max.
SELECT key, count() FROM t_agg_in_order_limit GROUP BY key ORDER BY key ASC LIMIT 5
SETTINGS optimize_aggregation_in_order = 1, extremes = 1;

DROP TABLE t_agg_in_order_limit;
