#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- the test pins the buffer/prefetch settings, randomization would break it
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up).
# Inverse direction of 04337: here `prefetch_buffer_size` (16 MiB) is *larger* than the effective remote
# read buffer `max_read_buffer_size_remote_fs` (1 MiB). With `enable_filesystem_cache = 0` the remote
# reader allocates exactly `max_read_buffer_size_remote_fs` (1 MiB); `DiskObjectStorage::prepareRead`
# only raises the buffer to `prefetch_buffer_size` when a filesystem-cache stage that prefers a bigger
# buffer is active. `MergeTreePrefetchedReadPool` must charge its memory budget with that same 1 MiB
# buffer, not `prefetch_buffer_size`, otherwise it overcharges by 16x and rejects prefetches that in
# reality fit.
#
# The two scans below differ only in `filesystem_prefetch_max_memory_usage`. Below the 1 MiB effective
# buffer the pool must admit no prefetch task; between 1 MiB and 16 MiB it must admit one. If the
# scheduler charged `prefetch_buffer_size` (16 MiB) it would reject the second scan too.
#
# See 04337 for why the admission decision is observed via
# `asynchronous_read_counters['total_prefetch_tasks']` and why `filesystem_prefetch_step_marks = 1` and
# `allow_prefetched_read_pool_for_remote_filesystem = 1` are required.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

-- One wide-part column whose compressed size (~32 MiB, CODEC(NONE)) exceeds every buffer size below, so
-- the per-prefetch estimate is bounded by the buffer size, not the column size.
CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0
AS SELECT repeat('a', 1024) FROM numbers_mt(32e3) SETTINGS enable_filesystem_cache = 0;

SET max_threads = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_prefetch = 1,
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    max_read_buffer_size_remote_fs = '1Mi',
    prefetch_buffer_size = '16Mi',
    filesystem_prefetch_step_marks = 1;

-- Budget below the 1 MiB effective buffer: the pool must admit no prefetch task.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '512Ki', log_comment = '04538_deny_budget_below_buffer';
-- Budget between the 1 MiB effective buffer and the 16 MiB prefetch_buffer_size: the pool admits a
-- prefetch task, because the real buffer is 1 MiB. If admission charged prefetch_buffer_size this would
-- be rejected.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '4Mi', log_comment = '04538_admit_budget_above_buffer';

SYSTEM FLUSH LOGS query_log;

-- max() over each log_comment collapses any repeats from the flaky check (the same test re-run in the
-- same database); the per-comment value is deterministic, so duplicates are harmless.
SELECT log_comment, max(asynchronous_read_counters['total_prefetch_tasks'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04538_deny_budget_below_buffer', '04538_admit_budget_above_buffer')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE test;
"
