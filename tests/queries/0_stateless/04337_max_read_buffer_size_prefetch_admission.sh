#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- the test pins the buffer/prefetch settings, randomization would break it
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up).
# The async prefetch buffer is allocated at the effective remote read-buffer size, which
# `Context::getReadSettings` caps at `DBMS_MAX_READ_BUFFER_SIZE` (16 MiB), independently of
# `prefetch_buffer_size`. `MergeTreePrefetchedReadPool` must charge its memory budget with that same
# effective buffer size, so `filesystem_prefetch_max_memory_usage` actually bounds the admitted
# prefetch memory.
#
# Here `prefetch_buffer_size` is tiny (1 byte) but `max_read_buffer_size_remote_fs` is 16 MiB, so each
# admitted prefetch reserves ~16 MiB. If the scheduler charged `prefetch_buffer_size` instead it would
# estimate ~1 byte and admit prefetches regardless of the budget. The two scans below differ only in
# `filesystem_prefetch_max_memory_usage`: below the 16 MiB effective buffer the pool must admit no
# prefetch task, above it the pool admits one.
#
# The admission decision is observed via `asynchronous_read_counters['total_prefetch_tasks']`, which
# counts the prefetches the pool actually schedules. `RemoteFSPrefetchedReads` is unsuitable here: a
# reader prefetches the next range of its own task during `MergeTreeReaderWide::readRows` regardless of
# the pool budget (gated only by `remote_filesystem_read_prefetch`), so it stays > 0 even when the pool
# admits nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

-- One wide-part column whose compressed size (~32 MiB, CODEC(NONE)) exceeds the 16 MiB buffer, so the
-- per-prefetch estimate is bounded by the buffer size, not the column size.
CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0
AS SELECT repeat('a', 1024) FROM numbers_mt(32e3) SETTINGS enable_filesystem_cache = 0;

SET max_threads = 1,
    remote_filesystem_read_prefetch = 1,
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    max_read_buffer_size_remote_fs = '16Mi',
    prefetch_buffer_size = 1;

-- Budget below the 16 MiB effective buffer: the pool must admit no prefetch task.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '1Mi', log_comment = '04337_deny_budget_below_buffer';
-- Budget above the 16 MiB effective buffer: the pool admits a prefetch task.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '64Mi', log_comment = '04337_admit_budget_above_buffer';

SYSTEM FLUSH LOGS query_log;

-- max() over each log_comment collapses any repeats from the flaky check (the same test re-run in the
-- same database); the per-comment value is deterministic, so duplicates are harmless.
SELECT log_comment, max(asynchronous_read_counters['total_prefetch_tasks'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('04337_deny_budget_below_buffer', '04337_admit_budget_above_buffer')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE test;
"
