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

-- Pin the single-stream path. The row-exact limit_hint check lives in
-- AggregatingInOrderTransform (cur_block_size + res_rows >= limit_hint); the multi-stream
-- FinishAggregatingInOrderTransform enforces the limit per finalized group batch
-- (finalized_group_batches >= limit_hint) and can overshoot into the whole a = 0 tie group
-- even with a broken guard. max_threads = 1 alone does not force this: in-order reads open
-- one ordered stream per part, so we also rely on the single INSERT above (one part). Assert
-- the pipeline is on AggregatingInOrderTransform with no FinishAggregatingInOrderTransform, so
-- removing the WITH TIES guard deterministically collapses the first query to 3, not 100.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0
   AND countIf(explain LIKE '%FinishAggregatingInOrderTransform%') = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
    SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1
);

-- Push-down enabled: the WITH TIES guard must keep the full tie group (100 rows, not 3).
SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1;

-- Push-down disabled: control producing the same 100 rows.
SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0, max_threads = 1;

DROP TABLE t_agg_in_order_limit_with_ties;
