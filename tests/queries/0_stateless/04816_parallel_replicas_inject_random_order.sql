-- With `inject_random_order_for_select_without_order_by` enabled, the `ORDER BY rand()` wrapper is injected
-- only into the top-level query (planned to stage `Complete`). The parallel replicas fragment is built from
-- the inner, unwrapped query tree, so replicas return partially aggregated blocks with `AggregatedChunkInfo`
-- and the initiator merges them; the injected wrapper must not break this or change the results.

DROP TABLE IF EXISTS t_pr_inject;

CREATE TABLE t_pr_inject (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_pr_inject SELECT number FROM numbers(100000);

SET inject_random_order_for_select_without_order_by = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- Global aggregation, no ORDER BY: the wrapper is injected at the top level.
SELECT sum(x), count() FROM t_pr_inject;

-- Real GROUP BY aggregation: each group must appear exactly once (no unmerged per-replica partials).
SELECT count(), sum(s) FROM (SELECT sum(x) AS s FROM t_pr_inject GROUP BY x % 7);

-- Non-aggregate query for completeness.
SELECT count() FROM (SELECT x FROM t_pr_inject WHERE x < 1000);

DROP TABLE t_pr_inject;
