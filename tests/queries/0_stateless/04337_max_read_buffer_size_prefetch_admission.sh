#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- the test pins prefetch buffer/memory settings, randomization would break it
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/84279 (follow-up).
# A single prefetch read buffer is capped at DBMS_MAX_READ_BUFFER_SIZE (16 MiB). The prefetch
# scheduler (MergeTreePrefetchedReadPool) must charge its memory budget with the same capped value.
# Here prefetch_buffer_size is set far above the cap (1 GiB) while filesystem_prefetch_max_memory_usage
# (32 MiB) fits the capped 16 MiB per-prefetch estimate but not the raw 1 GiB. If the scheduler used
# the raw value it would refuse to prefetch (RemoteFSPrefetchedReads = 0); with the capped value the
# prefetches are admitted (RemoteFSPrefetchedReads > 0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

-- One wide-part column whose compressed size (~100 MiB, CODEC(NONE)) far exceeds the 16 MiB cap, so
-- the per-prefetch estimate differs between the capped (16 MiB) and raw (1 GiB) buffer size.
CREATE TABLE test (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0
AS SELECT repeat('a', 1024) FROM numbers_mt(100e3) SETTINGS enable_filesystem_cache = 0;

SET max_threads = 1,
    remote_filesystem_read_prefetch = 1,
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 0,
    use_uncompressed_cache = 0,
    prefetch_buffer_size = '1Gi',
    filesystem_prefetch_max_memory_usage = '32Mi';

SELECT * FROM test FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RemoteFSPrefetchedReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
ORDER BY event_time_microseconds DESC
LIMIT 1;
"
