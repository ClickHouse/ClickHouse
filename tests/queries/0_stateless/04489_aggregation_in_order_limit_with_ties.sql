-- Tags: no-random-settings

-- LIMIT ... WITH TIES over aggregation in order must keep the whole tie group.

DROP TABLE IF EXISTS t_agg_in_order_limit_with_ties;

CREATE TABLE t_agg_in_order_limit_with_ties (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

INSERT INTO t_agg_in_order_limit_with_ties SELECT number % 10 AS a, number AS b FROM numbers(1000) ORDER BY a, b;

-- The single-stream path is what enforces the limit row-exactly; the multi-stream one can
-- overshoot into the whole tie group on its own, so pin the pipeline before asserting counts.
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0
   AND countIf(explain LIKE '%FinishAggregatingInOrderTransform%') = 0
FROM (
    EXPLAIN PIPELINE
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
    SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1
);

SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 1, max_threads = 1;

SELECT count() FROM (
    SELECT a, count() FROM t_agg_in_order_limit_with_ties GROUP BY a, b ORDER BY a ASC LIMIT 3 WITH TIES
) SETTINGS optimize_aggregation_in_order = 1, optimize_aggregation_in_order_limit = 0, max_threads = 1;

DROP TABLE t_agg_in_order_limit_with_ties;
