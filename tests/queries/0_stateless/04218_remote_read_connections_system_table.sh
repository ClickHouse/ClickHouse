#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-object-storage, no-random-settings
#
# no-fasttest -- needs an S3-backed disk
# no-parallel -- toggles a process-global failpoint

# system.remote_read_connections must surface the live remote connections the
# ReaderExecutor holds. The previous test only asserted count() >= 0 over a local
# MergeTree, which never opens a remote connection, so it would pass even if
# fillData returned nothing. Here a cold S3 scan is paused mid-read - with its
# live SourceBufferLimit connection still held - via the
# reader_executor_pause_after_window failpoint, and we assert the table reports a
# populated row (object path + elapsed time) for that query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Stable schema contract of the table.
$CLICKHOUSE_CLIENT --query "
    SELECT name, type
    FROM system.columns
    WHERE database = 'system' AND table = 'remote_read_connections'
    ORDER BY position"

FP="reader_executor_pause_after_window"
BG_ID="04218_re_conn_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_conn"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_conn (k UInt32, v String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_conn SELECT number, randomPrintableASCII(100) FROM numbers(300000)"

# Cold read: the scan misses the cache, opens a live connection to S3, and the
# failpoint pauses it after the first window with that connection still held.
$CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

$CLICKHOUSE_CLIENT --use_reader_executor=1 --query_id "$BG_ID" --query "
    SELECT count() FROM t_re_conn WHERE NOT ignore(v) FORMAT Null" > /dev/null 2>&1 &
SELECT_PID=$!

if timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"; then
    # A real remote ReaderExecutor connection is held; the system table must
    # report it for our query with a populated object path and non-negative
    # elapsed time.
    $CLICKHOUSE_CLIENT --query "
        SELECT 'connection_visible'
        WHERE EXISTS (
            SELECT 1 FROM system.remote_read_connections
            WHERE query_id = '$BG_ID' AND length(object_path) > 0 AND elapsed_seconds >= 0)"
else
    echo "FAIL: scan did not reach the pause failpoint"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait "$SELECT_PID"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_conn"
