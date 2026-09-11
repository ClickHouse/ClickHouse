-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

-- Both aggregation paths under test need more than one aggregating stream, and the first one also
-- needs the hash table to stay single-level, so the merge that records the statistics is the
-- single-level one. All three are randomized in CI.
SET max_threads=4;
SET group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;

DROP TABLE IF EXISTS oba_u8;
DROP TABLE IF EXISTS oba_u32;
DROP TABLE IF EXISTS oba_part;

-- `UInt8` keys aggregate into a fixed-size hash table, whose merge is parallelized across the
-- aggregating threads. `UInt32` keys hold the same 251 groups in an ordinary hash table and merge
-- serially, so the two tables must produce the same aggregate states and nearly the same estimate.
CREATE TABLE oba_u8 (g UInt8, v Float64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT toUInt8(number % 251), number * 1.5 FROM numbers(1000000);

CREATE TABLE oba_u32 (g UInt32, v Float64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT toUInt32(number % 251), number * 1.5 FROM numbers(1000000);

SET optimize_aggregation_in_order=0;
SELECT g, sum(v) FROM oba_u8 GROUP BY g FORMAT Null SETTINGS log_comment='oba_fixed_hash_map';
SELECT g, sum(v) FROM oba_u32 GROUP BY g FORMAT Null SETTINGS log_comment='oba_ordinary_hash_map';

-- Aggregating each partition independently drops the merging step that normally records the
-- output, so the aggregating transforms have to record it themselves.
CREATE TABLE oba_part (p UInt32, k UInt32, v Float64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, k)
AS SELECT number % 8, number, number * 1.5 FROM numbers(1000000);

SET optimize_aggregation_in_order=1, allow_aggregate_partitions_independently=1, force_aggregate_partitions_independently=1;
SELECT p, sum(v) FROM oba_part GROUP BY p FORMAT Null SETTINGS log_comment='oba_skip_merging';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE oba_u8;
DROP TABLE oba_u32;
DROP TABLE oba_part;

SYSTEM FLUSH LOGS query_log;

-- The aggregate states are two thirds of what these queries ship, so dropping them - or dropping
-- the whole estimate, which is what the independently-aggregated partitions used to do - leaves a
-- number the cost model reads as "shipping the result is free".
WITH map(
    'oba_fixed_hash_map',    2286,
    'oba_ordinary_hash_map', 3042,
    'oba_skip_merging',       512) AS expected
SELECT format('{} {} {}', log_comment, output_bytes, expected[log_comment])
FROM (
    SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'oba_%') AND (type = 'QueryFinish')
    ORDER BY event_time_microseconds
)
WHERE output_bytes = 0
   OR greatest(output_bytes, expected[log_comment]) / nullIf(least(output_bytes, expected[log_comment]), 0) > 2;
