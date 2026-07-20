-- Regression test: the parallel-replicas custom-key filter is a hidden reader-side filter, so the
-- trivial-`LIMIT` source cap (`max_block_size_limited` / `trivial_limit`) must be suppressed when it
-- applies, exactly like for a row policy or `additional_table_filters`. This models the replica-side
-- planning directly: the initiator dispatches the query with `parallel_replicas_count` and
-- `parallel_replica_offset` set, and the replica appends a `WHERE` filter that discards the rows
-- outside its share of the key space.
--
-- The result rows stay correct on `MergeTree` either way (the read is pulled lazily, so the source
-- cap does not truncate the surviving rows), so the discriminator is the read signature: with the
-- cap suppressed, the single pinned-size block covers the whole pruned range (hundreds of rows);
-- with the cap wrongly applied, `max_block_size` collapses to the `LIMIT` and only a handful of
-- rows are read before the `LIMIT` is satisfied. This is a companion to
-- `04558_parallel_replicas_stateful_limit_row_policy` (the row-policy flavor of the same check).

DROP TABLE IF EXISTS t_04620;
CREATE TABLE t_04620 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_04620 SELECT number FROM numbers(1000);

-- This replica owns the second half of the key range: the custom-key filter discards the first
-- 500 rows, so the trivial-`LIMIT` source cap in front of it would be unsafe.
SELECT k FROM t_04620 LIMIT 3 SETTINGS
    max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000, send_logs_level = 'fatal',
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_only_with_analyzer = 0,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 1, log_comment = '04620_new_off1';

SELECT k FROM t_04620 LIMIT 3 SETTINGS
    max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000, send_logs_level = 'fatal',
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_only_with_analyzer = 0,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 0,
    enable_analyzer = 1;

-- The old analyzer collects the custom-key filter into `query_info.filter_asts`, which already
-- suppresses the source cap (`maxBlockSizeByLimit` checks `filter_asts.empty()`); covered here so
-- both analyzers keep returning the correct rows.
SELECT k FROM t_04620 LIMIT 3 SETTINGS
    max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000, send_logs_level = 'fatal',
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_only_with_analyzer = 0,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 0;

SELECT k FROM t_04620 LIMIT 3 SETTINGS
    max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000, send_logs_level = 'fatal',
    preferred_max_column_in_block_size_bytes = 0, optimize_move_to_prewhere = 0,
    automatic_parallel_replicas_mode = 0, parallel_replicas_only_with_analyzer = 0,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 0,
    enable_analyzer = 0;

SYSTEM FLUSH LOGS query_log;

-- With the source cap suppressed, the (pinned-size) first block covers the whole pruned range,
-- so the read is a few hundred rows; with the cap wrongly applied, `max_block_size` collapses to
-- the `LIMIT` and only a handful of rows are read.
SELECT 'uncapped_read', read_rows > 100
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = '04620_new_off1'
ORDER BY event_time_microseconds DESC
LIMIT 1
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_04620;
