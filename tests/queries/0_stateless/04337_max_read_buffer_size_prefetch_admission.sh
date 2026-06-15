#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- the test pins prefetch buffer/memory settings, randomization would break it
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up).
# A remote prefetch buffer is allocated at remote_fs_settings.buffer_size (from
# max_read_buffer_size_remote_fs, capped at DBMS_MAX_READ_BUFFER_SIZE = 16 MiB), independently of
# prefetch_buffer_size. The prefetch scheduler (MergeTreePrefetchedReadPool) must charge its memory
# budget with that same effective buffer size, so filesystem_prefetch_max_memory_usage actually bounds
# the admitted prefetch memory.
#
# Here prefetch_buffer_size is tiny (1 byte) but max_read_buffer_size_remote_fs is 16 MiB, so each
# admitted prefetch allocates ~16 MiB. If the scheduler accounted prefetch_buffer_size it would
# estimate ~1 byte and admit prefetches regardless of the budget. The two queries below differ only in
# filesystem_prefetch_max_memory_usage: below the 16 MiB buffer the scheduler must NOT admit prefetches
# (RemoteFSPrefetchedReads = 0), above it the prefetches are admitted (> 0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

-- One wide-part column whose compressed size (~100 MiB, CODEC(NONE)) far exceeds the 16 MiB buffer,
-- so the per-prefetch estimate is bounded by the buffer size, not the column size.
CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0
AS SELECT repeat('a', 1024) FROM numbers_mt(100e3) SETTINGS enable_filesystem_cache = 0;

SET max_threads = 1,
    remote_filesystem_read_prefetch = 1,
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    max_read_buffer_size_remote_fs = '16Mi',
    prefetch_buffer_size = 1;

-- Budget below the 16 MiB async buffer: prefetches must be denied.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '1Mi', log_comment = '04337_deny_budget_below_buffer';
-- Budget above the 16 MiB async buffer: prefetches are admitted.
SELECT * FROM test FORMAT Null SETTINGS filesystem_prefetch_max_memory_usage = '64Mi', log_comment = '04337_admit_budget_above_buffer';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RemoteFSPrefetchedReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04337_%'
ORDER BY log_comment;
"
