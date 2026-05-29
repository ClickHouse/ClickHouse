#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings

# The ReaderExecutor bypasses the legacy `AsynchronousBoundedReadBuffer` wrapper
# that populates `system.filesystem_read_prefetches_log`. With
# `enable_filesystem_read_prefetches_log=1` the executor must emit its own
# prefetch-log rows, so the same remote read still produces log entries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_prefetchlog"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_prefetchlog (key UInt32, value String)
    ENGINE = MergeTree() ORDER BY key
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0
"
# Incompressible data spanning many read windows so prefetch has work to do.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_prefetchlog SELECT number, randomPrintableASCII(100) FROM numbers(300000)
"

QUERY_ID="04301_re_prefetchlog_${CLICKHOUSE_DATABASE}"

# Cold scan with the executor + prefetch + prefetch logging all enabled.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT \
    --query_id "$QUERY_ID" \
    --use_reader_executor=1 \
    --remote_filesystem_read_prefetch=1 \
    --enable_filesystem_read_prefetches_log=1 \
    --enable_reader_executor_log=1 \
    --query "SELECT count(), sum(cityHash64(value)) FROM t_re_prefetchlog FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS reader_executor_log, filesystem_read_prefetches_log"

# The executor actually ran (not the legacy path): a reader_executor_log row exists.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        count() = 0,
        'no reader_executor_log row: the ReaderExecutor did not run for this query')
    FROM system.reader_executor_log
    WHERE query_id = '$QUERY_ID'
    FORMAT Null
"

# The executor emitted prefetch-log rows: the contract is preserved. Every row
# must carry this query_id, a non-empty path, and a recognized prefetch state.
$CLICKHOUSE_CLIENT --query "
    SELECT throwIf(
        count() = 0
        OR countIf(path = '') != 0
        OR countIf(state NOT IN ('USED', 'CANCELLED_WITH_SEEK', 'CANCELLED_WITH_RANGE_CHANGE', 'UNNEEDED')) != 0,
        'ReaderExecutor produced no valid filesystem_read_prefetches_log rows despite enable_filesystem_read_prefetches_log=1')
    FROM system.filesystem_read_prefetches_log
    WHERE query_id = '$QUERY_ID'
    FORMAT Null
"
echo OK

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_prefetchlog"
