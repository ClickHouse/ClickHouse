-- Tags: no-random-settings, no-parallel-replicas

-- Correctness test for LIMIT push-down into aggregation-in-order under parallel
-- replicas. Under parallel replicas the cross-replica merge is a
-- MergingAggregated step over non-final per-replica Aggregating steps, so the
-- optimization (which requires a *final* in-order AggregatingStep and refuses
-- to cross a row-reducing MergingAggregated) does not fire on the distributed
-- path. This test pins that down: results must be identical with the
-- optimization on and off while parallel replicas are active.

DROP TABLE IF EXISTS t_agg_in_order_limit_pr;

CREATE TABLE t_agg_in_order_limit_pr (key UInt64, value UInt64)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 16;

-- 1000 distinct keys, 10 rows each, spread over many granules so the in-order
-- coordinator splits the key range across replicas.
INSERT INTO t_agg_in_order_limit_pr
SELECT number % 1000 AS key, number AS value
FROM numbers(10000)
ORDER BY key;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1,
    parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    optimize_aggregation_in_order = 1;

-- Result with push-down ON must equal result with push-down OFF.
SELECT
(
    SELECT groupArray((key, c, s))
    FROM
    (
        SELECT key, count() AS c, sum(value) AS s
        FROM t_agg_in_order_limit_pr
        GROUP BY key
        ORDER BY key ASC
        LIMIT 17
        SETTINGS optimize_aggregation_in_order_limit = 1
    )
)
=
(
    SELECT groupArray((key, c, s))
    FROM
    (
        SELECT key, count() AS c, sum(value) AS s
        FROM t_agg_in_order_limit_pr
        GROUP BY key
        ORDER BY key ASC
        LIMIT 17
        SETTINGS optimize_aggregation_in_order_limit = 0
    )
) AS results_match;

-- With OFFSET as well.
SELECT
(
    SELECT groupArray((key, c, s))
    FROM
    (
        SELECT key, count() AS c, sum(value) AS s
        FROM t_agg_in_order_limit_pr
        GROUP BY key
        ORDER BY key ASC
        LIMIT 11 OFFSET 23
        SETTINGS optimize_aggregation_in_order_limit = 1
    )
)
=
(
    SELECT groupArray((key, c, s))
    FROM
    (
        SELECT key, count() AS c, sum(value) AS s
        FROM t_agg_in_order_limit_pr
        GROUP BY key
        ORDER BY key ASC
        LIMIT 11 OFFSET 23
        SETTINGS optimize_aggregation_in_order_limit = 0
    )
) AS results_match_offset;

-- Pin down the actual values too, so the reference is not vacuously satisfied.
SELECT key, count() AS c, sum(value) AS s
FROM t_agg_in_order_limit_pr
GROUP BY key
ORDER BY key ASC
LIMIT 5
SETTINGS optimize_aggregation_in_order_limit = 1;

DROP TABLE t_agg_in_order_limit_pr;
