#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel-replicas
# - no-random-settings -- the test pins the buffer/prefetch settings, randomization would break it
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up), local path.
# `MergeTreePrefetchedReadPool` also serves all-local reads (when
# `allow_prefetched_read_pool_for_local_filesystem` is enabled), and a local prefetched reader
# allocates `local_fs_settings.buffer_size` (from `max_read_buffer_size_local_fs`), not the remote
# buffer. So the pool must charge its `filesystem_prefetch_max_memory_usage` budget with the local
# effective buffer for local parts; charging the remote estimate would admit a local prefetch on a
# smaller number while each reader allocates the larger local buffer.
#
# Here the local effective buffer is 16 MiB while the remote/prefetch sizes are tiny (1 MiB / 1 byte),
# so the two estimates diverge: a budget below 16 MiB must deny, above it must admit -- and a scheduler
# that charged the remote estimate (~1 MiB) would wrongly admit the 8 MiB-budget case.
#
# Notes:
# - The pool's `MergeTreePrefetchedReadPool::checkReadMethodAllowed` only allows the async local
#   methods, hence `local_filesystem_read_method = 'pread_threadpool'`.
# - The prefetched pool is taken for local parts only with more than one stream (a single-stream local
#   read goes through the plain reader), so the table has two parts and `max_threads = 2`.
# - `allow_prefetched_read_pool_for_local_filesystem = 1` overrides the stateless default that disables
#   the pool (tests/config/users.d/prefetch_settings.xml).
# - `disk = 'default'` pins the table to the local disk so the test exercises the local read path on
#   every CI configuration. Some configs override the default MergeTree `storage_policy` to object
#   storage (e.g. tests/config/config.d/s3_storage_policy_for_merge_tree_by_default.xml); without this
#   the parts land on S3 and the remote estimate (~1 MiB) wrongly admits the 8 MiB-budget deny case.
# - `filesystem_prefetch_step_marks = 1` gives the pool look-ahead tasks to pre-schedule.
# - Admission is observed via `asynchronous_read_counters['total_prefetch_tasks']` (counts only
#   pool-scheduled prefetches); `max()` per log_comment collapses any flaky-check rerun duplicates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

-- Two wide local parts, each compressed size (~20 MiB, CODEC(NONE)) above the 16 MiB local buffer, so
-- the per-prefetch estimate is bounded by the buffer, not the column size. Two parts give the pool
-- more than one stream so it is actually selected for the local read.
CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 'default', min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT repeat('a', 1024) FROM numbers(20000);
INSERT INTO test SELECT repeat('a', 1024) FROM numbers(20000);

SET allow_prefetched_read_pool_for_local_filesystem = 1,
    local_filesystem_read_method = 'pread_threadpool',
    local_filesystem_read_prefetch = 1,
    max_threads = 2,
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    max_read_buffer_size_local_fs = '16Mi',
    max_read_buffer_size_remote_fs = '1Mi',
    prefetch_buffer_size = 1,
    filesystem_prefetch_step_marks = 1;

-- Budget below the 16 MiB local effective buffer: the pool must admit no prefetch task.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '8Mi', log_comment = '04338_deny_local_budget_below_buffer';
-- Budget above the 16 MiB local effective buffer: the pool admits a prefetch task.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '64Mi', log_comment = '04338_admit_local_budget_above_buffer';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, max(asynchronous_read_counters['total_prefetch_tasks'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04338_deny_local_budget_below_buffer', '04338_admit_local_budget_above_buffer')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE test;
"
