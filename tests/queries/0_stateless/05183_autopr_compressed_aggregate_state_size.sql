-- For runs with the old analyzer
SET enable_analyzer=1;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;
SET use_query_condition_cache=0;

-- Both randomized in CI: the aggregation needs more than one stream, and it has to stay single-level
-- so the states are estimated off one hash table.
SET max_threads=4;
SET group_by_two_level_threshold=0, group_by_two_level_threshold_bytes=0;

DROP TABLE IF EXISTS asz_t;

-- `same` is constant, so every group's aggregate states hold the same handful of values and compress
-- to little. `varied` is a hash, so each group's states differ and barely compress at all. The two
-- queries below group the same keys with the same four aggregate functions, so their estimates differ
-- only by how well the states compress.
CREATE TABLE asz_t (g UInt32, same Float64, varied Float64) ENGINE = MergeTree ORDER BY tuple()
AS SELECT toUInt32(number % 60000), 1.0, toFloat64(cityHash64(number)) FROM numbers(1000000);

SELECT g, sum(same), min(same), max(same), any(same) FROM asz_t GROUP BY g FORMAT Null
    SETTINGS log_comment='asz_identical_states';
SELECT g, sum(varied), min(varied), max(varied), any(varied) FROM asz_t GROUP BY g FORMAT Null
    SETTINGS log_comment='asz_varied_states';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

DROP TABLE asz_t;

SYSTEM FLUSH LOGS query_log;

-- `Aggregator::estimateSizeOfCompressedState` serializes sampled states through a
-- `CompressedWriteBuffer`, so compressible states have to cost visibly less than incompressible ones.
-- When the serialization bypasses that buffer the two queries report the same size and this ratio
-- collapses to 1.
WITH stats AS (
    SELECT log_comment AS lc, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] AS output_bytes
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
      AND (current_database = currentDatabase()) AND (log_comment LIKE 'asz_%') AND (type = 'QueryFinish')
)
SELECT format('identical={} varied={}', identical, varied)
FROM (
    SELECT sumIf(output_bytes, lc = 'asz_identical_states') AS identical,
           sumIf(output_bytes, lc = 'asz_varied_states') AS varied
    FROM stats
)
WHERE identical = 0 OR varied = 0 OR (varied / identical) < 3;
