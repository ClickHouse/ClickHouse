-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

-- Aggregating each partition independently skips the merge phase where the global
-- `max_rows_to_group_by` limit is enforced, so `optimizeAggregationPerPartition` falls back to normal
-- aggregation whenever that limit is set. The stateless test profile
-- (`tests/config/users.d/limits.yaml`) sets a high `max_rows_to_group_by` as a safety net, which would
-- leave the query below on the ordinary merging pipeline and make it assert nothing.
SET max_rows_to_group_by = 0;

-- Both queries need more than one aggregating stream, and `max_threads` is randomized in CI.
SET max_threads=4;

-- The bucket-local Top-K materializes only each two-level bucket's best n groups, and is what these
-- queries are about, so it is pinned rather than left to `query_plan_enable_optimizations`
-- randomization. `optimize_aggregation_in_order` is pinned off to keep both queries on the unordered
-- pipeline, and `group_by_two_level_threshold` decides single- vs two-level aggregation, which the
-- Top-K conversion is a property of.
SET query_plan_aggregation_bucket_top_k=1, optimize_aggregation_in_order=0, group_by_two_level_threshold=1000;

DROP TABLE IF EXISTS obt_part;

-- The partition key is a function of the group keys, so aggregating each partition independently is
-- allowed. A million groups against `LIMIT 10` make the truncation drastic: each of the 256 buckets
-- of each stream's own table keeps at most ten groups.
CREATE TABLE obt_part (p UInt8, k UInt32, v Float64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, k)
AS SELECT number % 8, number, number * 1.5 FROM numbers(1000000);

SELECT p, k, count() FROM obt_part GROUP BY p, k ORDER BY count() DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='obt_merged', allow_aggregate_partitions_independently=0;

SELECT p, k, count() FROM obt_part GROUP BY p, k ORDER BY count() DESC LIMIT 10 FORMAT Null
    SETTINGS log_comment='obt_skip_merging',
             allow_aggregate_partitions_independently=1, force_aggregate_partitions_independently=1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE obt_part;

SYSTEM FLUSH LOGS query_log;

-- `RuntimeDataflowStatisticsOutputBytes` must describe the untruncated aggregation output, because it
-- prices the shipping term of the parallel-replicas plan, whose partial aggregation materializes every
-- group. Skipping the merge converts the buckets through a different source than the merging pipeline
-- does, and that source has to price the groups the Top-K threw away just as the merging one does, so
-- the two are compared against each other: they see the same groups and the same states, and a source
-- that measured the truncated chunk instead would fall orders of magnitude below its control rather
-- than slightly below it.
WITH
    stats AS (
        SELECT log_comment AS lc, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
        FROM system.query_log
        WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
          AND (current_database = currentDatabase()) AND (log_comment LIKE 'obt_%') AND (type = 'QueryFinish')
    ),
    merged AS (SELECT sumIf(output_bytes, lc = 'obt_merged') AS bytes FROM stats)
SELECT format('{} {} {}', lc, output_bytes, (SELECT bytes FROM merged))
FROM stats
WHERE output_bytes = 0
   OR ((SELECT bytes FROM merged) > 0
       AND greatest(output_bytes, (SELECT bytes FROM merged)) / least(output_bytes, (SELECT bytes FROM merged)) > 2)
ORDER BY lc;
