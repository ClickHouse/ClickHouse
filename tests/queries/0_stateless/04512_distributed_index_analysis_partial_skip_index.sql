-- Tags: no-random-merge-tree-settings, no-random-settings
-- - no-random-merge-tree-settings -- relies on a fixed mark layout (512 marks), the
--   distributed-index-analysis settings, and the compress-block/index-granule sizing below

DROP TABLE IF EXISTS t_dia_skip;

-- Every ngrambf granule is 8 KiB and the compress block is capped at 8 KiB, so each skip-index
-- granule lands in (at least) its own compressed block and a partial read of the index is
-- observable as fewer decompressed bytes.
CREATE TABLE t_dia_skip (key Int, s String, INDEX idx_s s TYPE ngrambf_v1(4, 8192, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY key
SETTINGS distributed_index_analysis_min_parts_to_activate = 0,
         distributed_index_analysis_min_indexes_bytes_to_activate = 0,
         distributed_index_analysis_mark_segment_size = 8,
         distributed_index_analysis_min_marks_to_split_part = 0,
         index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
         max_compress_block_size = 8192;

SYSTEM STOP MERGES t_dia_skip;
INSERT INTO t_dia_skip SELECT number, toString(sipHash64(number)) FROM numbers(65536);
-- Single part all_1_1_0 with 512 marks -> 64 mark segments of 8 marks spread across the replicas.

SET allow_experimental_parallel_reading_from_replicas = 0;
SET max_parallel_replicas = 100;
--- Ignore warnings when a replica does not respond, and analysis is done on the initiator
SET send_logs_level = 'error';

-- The predicate uses only the skip-index column (no primary-key condition), so all index reads
-- of the analysis queries below are skip-index reads.

-- Whole-part analysis: the baseline, reads all 512 skip-index granules.
SELECT empty(ranges) FROM mergeTreeAnalyzeIndexes(currentDatabase(), 't_dia_skip', s = 'no-such-value') WHERE part_name = 'all_1_1_0';

SELECT count() FROM t_dia_skip WHERE s = 'no-such-value'
SETTINGS cluster_for_parallel_replicas = 'parallel_replicas', distributed_index_analysis = 1;

SYSTEM FLUSH LOGS query_log;

-- Over the initiator query and the per-replica analysis queries it spawned: the remote analysis
-- queries carry per-segment mark ranges, and each analysis query decompresses only its segments'
-- skip-index granules - strictly less than the whole-part baseline (each replica holds only a
-- share of the 64 segments; 0.9 leaves slack for an uneven consistent-hash split).
WITH (
    SELECT ProfileEvents['CompressedReadBufferBytes']
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND query LIKE '%empty(ranges)%mergeTreeAnalyzeIndexes(%' AND query NOT LIKE '%query_log%'
) AS whole_part_bytes
SELECT
    countIf(query LIKE 'SELECT * FROM mergeTreeAnalyzeIndexesUUID%array((%') > 0 AS remote_segment_analysis,
    whole_part_bytes >= 512 * 8192 AS baseline_read_all_granules,
    countIf(ProfileEvents['CompressedReadBufferBytes'] > 0) > 0 AS analysis_read_something,
    max(ProfileEvents['CompressedReadBufferBytes']) < whole_part_bytes * 0.3 AS partial_skip_index_read
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND endsWith(log_comment, '-' || currentDatabase())
  AND (query LIKE '%mergeTreeAnalyzeIndexesUUID%' OR query LIKE '%SELECT count()%t_dia_skip%');

DROP TABLE t_dia_skip;
