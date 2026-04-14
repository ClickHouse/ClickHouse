-- Regression test: group_by_limit_pushdown with Distinct combinator caused
-- use-after-destroy when trimHeapAndPruneHashTable evicted keys whose
-- aggregate states were already referenced in the places[] array.
-- https://github.com/ClickHouse/ClickHouse/pull/96630

DROP TABLE IF EXISTS t_gbylimit_distinct;

CREATE TABLE t_gbylimit_distinct (k UInt32, val UInt64) ENGINE = MergeTree ORDER BY k;

-- Insert enough rows with many distinct keys to trigger the top-N heap trim
-- within a single batch.
INSERT INTO t_gbylimit_distinct SELECT number % 1000, number FROM numbers(10000);

-- The crash happened with skewSampDistinct, but any Distinct combinator on a
-- numeric column exercises the same code path (AggregateFunctionDistinctSingleNumericData).
-- Using a small LIMIT ensures group_by_limit_pushdown kicks in and the heap evicts keys.
SELECT k, skewSampDistinct(val)
FROM t_gbylimit_distinct
GROUP BY k
ORDER BY k
LIMIT 5
SETTINGS group_by_limit_pushdown = 1;

DROP TABLE t_gbylimit_distinct;
