-- Tags: no-parallel, no-random-merge-tree-settings, no-random-settings
-- - no-parallel -- asserts primary-index load counts after SYSTEM CLEAR PRIMARY INDEX CACHE
-- - no-random-merge-tree-settings -- relies on a fixed mark layout (512 marks) and on the
--   primary index cache and distributed-index-analysis settings being enabled

DROP TABLE IF EXISTS t_dia_ppk;

CREATE TABLE t_dia_ppk (key Int, value Int)
ENGINE = MergeTree ORDER BY key
SETTINGS distributed_index_analysis_min_parts_to_activate = 0,
         distributed_index_analysis_min_indexes_bytes_to_activate = 0,
         distributed_index_analysis_mark_segment_size = 8,
         distributed_index_analysis_min_marks_to_split_part = 0,
         use_primary_key_cache = 1, prewarm_primary_key_cache = 0,
         index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_dia_ppk;
INSERT INTO t_dia_ppk SELECT number, number FROM numbers(65536);
-- Single part all_1_1_0 with 512 marks -> 64 mark segments of 8 marks spread across the replicas.

SET allow_experimental_parallel_reading_from_replicas = 0;
SET max_parallel_replicas = 100;
--- Ignore warnings when a replica does not respond, and analysis is done on the initiator
SET send_logs_level = 'error';

SYSTEM CLEAR PRIMARY INDEX CACHE;

SELECT sum(key) FROM t_dia_ppk WHERE key >= 1000 AND key < 3000
SETTINGS cluster_for_parallel_replicas = 'parallel_replicas', distributed_index_analysis = 1;

SYSTEM FLUSH LOGS query_log;

-- Over the initiator query and the per-replica analysis queries it spawned:
-- - the remote analysis queries carry per-segment mark ranges (the tuple wire form);
-- - the primary index is loaded partially: the first loader populates the cache with only its
--   segments (later analysis queries may widen the entry up to the whole part, and covered
--   requests load nothing, so only bounds are asserted);
-- - no load ever exceeds the full index (512 rows).
SELECT
    countIf(query LIKE 'SELECT * FROM mergeTreeAnalyzeIndexesUUID%array((%') > 0 AS remote_segment_analysis,
    countIf(ProfileEvents['LoadedPrimaryIndexRows'] BETWEEN 1 AND 511) > 0 AS partial_pk_load,
    max(ProfileEvents['LoadedPrimaryIndexRows']) <= 512 AS at_most_full
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND endsWith(log_comment, '-' || currentDatabase()) -- analog of "current_database = currentDatabase()" for distributed queries
  AND (query LIKE '%mergeTreeAnalyzeIndexesUUID%' OR query LIKE '%t_dia_ppk%');

DROP TABLE t_dia_ppk;
