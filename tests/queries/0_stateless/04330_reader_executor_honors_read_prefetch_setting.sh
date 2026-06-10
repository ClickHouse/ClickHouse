#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# `remote_filesystem_read_prefetch` must gate the ReaderExecutor prefetch path:
# with `use_reader_executor=1`, the executor issues background prefetches only
# when `remote_filesystem_read_prefetch=1`, and none when `=0` (the prefetch
# pool is not attached, so `maybeTriggerPrefetch` is a no-op).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_prefetch"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_prefetch (key UInt32, value String)
    ENGINE = MergeTree() ORDER BY key
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0
"
# Incompressible data spanning many read windows so prefetch has work to do.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_prefetch SELECT number, randomPrintableASCII(100) FROM numbers(300000)
"

# Cold scan with prefetch DISABLED.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch=0 --query "
    SELECT 're_prefetch_off', count(), sum(cityHash64(value)) FROM t_re_prefetch FORMAT Null
"
# Cold scan with prefetch ENABLED.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch=1 --query "
    SELECT 're_prefetch_on', count(), sum(cityHash64(value)) FROM t_re_prefetch FORMAT Null
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# prefetch=0 -> no prefetch pool attached -> zero prefetch activity, while the
# executor still ran (read misses from source). The BytesFromSource guard makes
# this non-vacuous (fails if the executor path was not taken at all).
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        sum(ProfileEvents['ReaderExecutorPrefetchHits'] + ProfileEvents['ReaderExecutorPrefetchCancelled']) != 0
        OR sum(ProfileEvents['ReaderExecutorBytesFromSource']) = 0,
        'executor prefetched despite remote_filesystem_read_prefetch=0 (or executor did not run)')
    FROM system.query_log
    WHERE query LIKE '%re\\_prefetch\\_off%' AND type = 'QueryFinish' AND current_database = currentDatabase()
    FORMAT Null
"
# prefetch=1 -> each window submits a prefetch that the next read consumes (hit)
# or cancels, so prefetch activity must be non-zero.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        sum(ProfileEvents['ReaderExecutorPrefetchHits'] + ProfileEvents['ReaderExecutorPrefetchCancelled']) = 0,
        'executor did not prefetch despite remote_filesystem_read_prefetch=1')
    FROM system.query_log
    WHERE query LIKE '%re\\_prefetch\\_on%' AND type = 'QueryFinish' AND current_database = currentDatabase()
    FORMAT Null
"
echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_prefetch"
