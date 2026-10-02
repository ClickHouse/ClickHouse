-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

-- Aggregating each partition independently skips the merge phase where the global
-- `max_rows_to_group_by` limit is enforced, so both passes that set it (`optimizeAggregationPerPartition`
-- and `applyStreamDisjointness`) fall back to normal aggregation whenever that limit is set. The
-- stateless test profile (`tests/config/users.d/limits.yaml`) sets a high `max_rows_to_group_by` as a
-- safety net, which would leave the `oba_skip_merging_*` queries below on the ordinary merging
-- pipeline and make them assert nothing about the path they name.
SET max_rows_to_group_by = 0;

-- Every aggregation path under test needs more than one aggregating stream, and `max_threads` is
-- randomized in CI. `group_by_two_level_threshold` is randomized too and decides single- vs two-level
-- aggregation, so each query below pins it to the level it means to exercise.
SET max_threads=4;

DROP TABLE IF EXISTS oba_u16;
DROP TABLE IF EXISTS oba_u32;
DROP TABLE IF EXISTS oba_part;

-- `UInt8` and `UInt16` keys aggregate into a fixed-size hash table, whose merge is parallelized
-- across the aggregating threads. `UInt32` keys hold the same 60000 groups in an ordinary hash table
-- and merge serially. The aggregate states are the bulk of both results, so a path that records only
-- the group keys falls far below its baseline rather than slightly below it.
CREATE TABLE oba_u16 (g UInt16, v Float64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT toUInt16(number % 60000), number * 1.5 FROM numbers(1000000);

CREATE TABLE oba_u32 (g UInt32, v Float64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT toUInt32(number % 60000), number * 1.5 FROM numbers(1000000);

SELECT g, sum(v) FROM oba_u16 GROUP BY g FORMAT Null
    SETTINGS log_comment='oba_fixed_hash_map', group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;
SELECT g, sum(v) FROM oba_u32 GROUP BY g FORMAT Null
    SETTINGS log_comment='oba_ordinary_hash_map', group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;

-- Aggregating each partition independently drops the merging step that normally records the output,
-- in both the in-order and the unordered pipeline. One million groups of eight bytes of state keep
-- the estimate far above the per-block constants of the sampling.
CREATE TABLE oba_part (p UInt8, k UInt32, v Float64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, k)
AS SELECT number % 8, number, number * 1.5 FROM numbers(1000000);

SELECT p, k, sum(v) FROM oba_part GROUP BY p, k FORMAT Null
    SETTINGS log_comment='oba_merged', group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0,
             optimize_aggregation_in_order=0, allow_aggregate_partitions_independently=0;

SELECT p, k, sum(v) FROM oba_part GROUP BY p, k FORMAT Null
    SETTINGS log_comment='oba_skip_merging_single_level', group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0,
             optimize_aggregation_in_order=0, allow_aggregate_partitions_independently=1, force_aggregate_partitions_independently=1;

SELECT p, k, sum(v) FROM oba_part GROUP BY p, k FORMAT Null
    SETTINGS log_comment='oba_skip_merging_two_level', group_by_two_level_threshold=1000,
             optimize_aggregation_in_order=0, allow_aggregate_partitions_independently=1, force_aggregate_partitions_independently=1;

SELECT p, k, sum(v) FROM oba_part GROUP BY p, k FORMAT Null
    SETTINGS log_comment='oba_skip_merging_in_order', group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0,
             optimize_aggregation_in_order=1, allow_aggregate_partitions_independently=1, force_aggregate_partitions_independently=1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE oba_u16;
DROP TABLE oba_u32;
DROP TABLE oba_part;

SYSTEM FLUSH LOGS query_log;

-- Each cell is checked against another cell that prices its aggregate states the same way, rather
-- than against a byte count, so the check says only that the states were counted - not how large a
-- compressed state happens to be:
--   * the three unordered pipelines all price states with `Aggregator::estimateSizeOfCompressedState`,
--     so skipping the merge must land within 2x of the merged control;
--   * `UInt16` and `UInt32` keys hold the same groups and the same states, so the fixed-size hash
--     table must land within 2x of the ordinary one;
--   * `AggregatingInOrderTransform` prices states by compressing the state columns instead, which is
--     a different scale entirely, so that cell only has to stay far above zero.
WITH
    stats AS (
        SELECT log_comment AS lc, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
        FROM system.query_log
        WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
          AND (current_database = currentDatabase()) AND (log_comment LIKE 'oba_%') AND (type = 'QueryFinish')
    ),
    reference AS (
        SELECT sumIf(output_bytes, lc = 'oba_merged') AS merged,
               sumIf(output_bytes, lc = 'oba_ordinary_hash_map') AS ordinary
        FROM stats
    )
SELECT format('{} {} {}', lc, output_bytes, ref)
FROM (
    SELECT lc, output_bytes,
           multiIf(lc = 'oba_fixed_hash_map', (SELECT ordinary FROM reference),
                   lc IN ('oba_skip_merging_single_level', 'oba_skip_merging_two_level'), (SELECT merged FROM reference),
                   0) AS ref
    FROM stats
)
WHERE output_bytes = 0
   OR (ref > 0 AND greatest(output_bytes, ref) / least(output_bytes, ref) > 2)
   OR (lc = 'oba_skip_merging_in_order' AND output_bytes < 1000000)
ORDER BY lc;
