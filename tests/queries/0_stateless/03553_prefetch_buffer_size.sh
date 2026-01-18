#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, no-parallel-replicas
# - no-random-settings -- prefetch became non deterministic
# - no-fasttest -- requires S3
# - no-parallel-replicas -- query can be executed on another node

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
DROP TABLE IF EXISTS test;

CREATE TABLE test_1 (s String CODEC(NONE)) ENGINE = MergeTree() ORDER BY ()
SETTINGS disk = disk(name = 'test_prefetch_buffer_size_$CLICKHOUSE_DATABASE', type = cache, max_size = '10Gi', path = 'test_prefetch_buffer_size_$CLICKHOUSE_DATABASE', disk = 's3_disk'), min_bytes_for_wide_part=0, serialization_info_version='basic'
AS SELECT repeat('a', 1024) FROM numbers_mt(20e3) SETTINGS enable_filesystem_cache = 0;

-- Need separate tables to avoid cache polution
CREATE TABLE test_2 AS test_1;
INSERT INTO test_2 SELECT * FROM test_1 SETTINGS enable_filesystem_cache = 0;
CREATE TABLE test_3 AS test_1;
INSERT INTO test_3 SELECT * FROM test_1 SETTINGS enable_filesystem_cache = 0;

-- Just one setting for more deterministic prefetch...
SET max_threads = 1,
    remote_filesystem_read_prefetch = 1,
    max_read_buffer_size_remote_fs = 128e3,
    filesystem_prefetch_max_memory_usage = '100Gi',
    remote_filesystem_read_method = 'threadpool',
    enable_filesystem_cache = 1,
    use_uncompressed_cache = 0
;

SELECT * FROM test_1 FORMAT Null SETTINGS filesystem_cache_prefer_bigger_buffer_size = 1, prefetch_buffer_size = 1_000_000;
SELECT * FROM test_2 FORMAT Null SETTINGS filesystem_cache_prefer_bigger_buffer_size = 1, prefetch_buffer_size = 10_000_000;
SELECT * FROM test_3 FORMAT Null SETTINGS filesystem_cache_prefer_bigger_buffer_size = 0, prefetch_buffer_size = 1_000_000;

SYSTEM FLUSH LOGS query_log;
SELECT formatQuerySingleLine(query), ProfileEvents['RemoteFSPrefetchedBytes'], ProfileEvents['RemoteFSPrefetchedReads'] FROM system.query_log WHERE current_database = currentDatabase() AND type != 'QueryStart' AND query_kind = 'Select' ORDER BY event_time_microseconds;
"
