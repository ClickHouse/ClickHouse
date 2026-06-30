-- Tags: no-random-settings

-- Negative test for LIMIT push-down into aggregation-in-order with LIMIT ... WITH TIES.
-- GROUP BY (a,b) ORDER BY a (a prefix of the group-by sort description) LIMIT n WITH TIES.
-- The aggregator must NOT stop after n group batches: WITH TIES extends the result to all
-- groups whose ORDER BY key equals the n-th row's key. Pushing the limit would truncate
-- those tie rows and return a short, wrong result. Result must match push-down on vs off.

DROP TABLE IF EXISTS t_agg_in_order_limit_with_ties;

CREATE TABLE t_agg_in_order_limit_with_ties (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

-- 10 distinct a values (0..9), 100 distinct (a,b) groups each. ORDER BY a LIMIT 3 WITH TIES
-- selects every group with a = 0 (the 3rd row's a), i.e. 100 rows.
INSERT INTO t_agg_in_order_limit_with_ties SELECT number % 10 AS a, number AS b FROM numbers(1000) ORDER BY a, b;

SELECT
(
    SELECT count() FROM (
        SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
    )
)
=
(
    SELECT count() FROM (
        SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
        SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0
    )
) AS with_ties_results_match;

-- Pin the actual row count so the equality above is not vacuously satisfied.
SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
    SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1
);

DROP TABLE t_agg_in_order_limit_with_ties;
