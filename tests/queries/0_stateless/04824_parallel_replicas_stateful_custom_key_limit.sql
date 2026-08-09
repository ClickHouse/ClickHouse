-- Regression test: the parallel-replicas custom-key filter is built by wrapping the user-provided
-- key expression in deterministic modulo / range comparisons, but the key expression itself is
-- arbitrary and can hold a stateful call (e.g. `parallel_replicas_custom_key = 'neighbor(k, 1)'`).
-- Such a filter runs on the read side, so the trivial-`LIMIT` fast path must raise the same
-- single-deterministic-stream requirement as for a stateful row policy or
-- `additional_table_filters` (see `04637_stateful_row_policy_trivial_limit`). A deterministic
-- custom key keeps the multi-stream read: only the source cap is suppressed for it.
-- This models the replica-side planning directly: the initiator dispatches the query with
-- `parallel_replicas_count` and `parallel_replica_offset` set, and the replica appends a filter
-- that discards the rows outside its share of the key space
-- (companion to `04620_parallel_replicas_custom_key_trivial_limit`).

-- The per-query `enable_analyzer = 1` below sits inside `FROM (EXPLAIN ...)` subqueries; pin the
-- analyzer at the session level too, or the old-analyzer CI configuration rejects the queries with
-- "Setting 'enable_analyzer' is changed in the subquery" (`INCORRECT_QUERY`).
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_04824;
CREATE TABLE t_04824 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_04824 SELECT number FROM numbers(1000);

-- A stateful custom key - a single stream.
SELECT 'custom key stateful', if(count() = 1 AND max(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64)) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_04824 LIMIT 1000 SETTINGS
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.,
    max_threads = 4, max_block_size = 65536,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    allow_deprecated_error_prone_window_functions = 1,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'neighbor(k, 1)',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 1)
WHERE explain LIKE '%MergeTreeSelect%';

-- A deterministic custom key - the read still uses several streams.
SELECT 'custom key plain', if(count() = 1 AND max(toUInt64OrDefault(extract(explain, '× (\d+)'), 1::UInt64)) = 1, 'single stream', 'multiple streams')
FROM (EXPLAIN PIPELINE SELECT k FROM t_04824 LIMIT 1000 SETTINGS
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.,
    max_threads = 4, max_block_size = 65536,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1,
    enable_parallel_replicas = 1, max_parallel_replicas = 2,
    parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_shard_localhost',
    parallel_replicas_count = 2, parallel_replica_offset = 1,
    enable_analyzer = 1)
WHERE explain LIKE '%MergeTreeSelect%';

DROP TABLE t_04824;
