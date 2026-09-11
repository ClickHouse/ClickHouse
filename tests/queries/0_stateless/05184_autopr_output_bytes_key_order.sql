-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

-- Both randomized in CI. The aggregation needs more than one stream to reach the merging transform
-- the estimate is sampled at, and `optimize_aggregation_in_order` is what the two queries below differ
-- by, so it cannot be left to the randomizer.
SET max_threads=4;
SET optimize_aggregation_in_order=0;
-- The replicas sort what they send only when memory-bound merging applies to it, so with this off the
-- estimate is right to leave the sample in hash order and the two queries below converge. It, the
-- two-level thresholds and the block sizing all decide which merge the estimate is sampled from, and
-- all of them are randomized in CI.
SET enable_memory_bound_merging_of_aggregation_results=1;
SET distributed_aggregation_memory_efficient=1;
SET group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;
SET max_block_size=65409, aggregation_in_order_max_block_bytes=50000000;

DROP TABLE IF EXISTS okey_t;

-- Monotonic `UInt64` keys, which compress about 7.8x in key order and about 2.4x in hash order, so the
-- two orders are far enough apart to tell from the estimate alone.
CREATE TABLE okey_t (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key
AS SELECT number, number FROM numbers(2000000);

-- Aggregating in order, the replicas merge their result with memory-bound merging, which sorts it by
-- the group by keys before sending. Plain aggregation sends it in hash-table order.
SELECT key, count() FROM okey_t GROUP BY key FORMAT Null
    SETTINGS log_comment='okey_in_order', optimize_aggregation_in_order=1;
SELECT key, count() FROM okey_t GROUP BY key FORMAT Null
    SETTINGS log_comment='okey_hash', optimize_aggregation_in_order=0, group_by_two_level_threshold=1000;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE okey_t;

SYSTEM FLUSH LOGS query_log;

-- The plan the statistics are sampled from does not sort - it is not producing results in bucket order
-- - so the sample has to be put into key order to be priced the way the replicas send it. Without that
-- both queries are measured in hash-table order and report the same size, and the sorted one is
-- overcharged by the whole difference between the two orders.
WITH stats AS (
    SELECT log_comment AS lc, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'okey_%') AND (type = 'QueryFinish')
)
SELECT format('in_order={} hash={}', in_order, hash)
FROM (
    SELECT sumIf(output_bytes, lc = 'okey_in_order') AS in_order,
           sumIf(output_bytes, lc = 'okey_hash') AS hash
    FROM stats
)
WHERE in_order = 0 OR hash = 0 OR (hash / in_order) < 2;
