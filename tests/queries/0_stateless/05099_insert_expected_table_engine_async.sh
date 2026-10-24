#!/usr/bin/env bash
# An asynchronous insert is checked again by its flush, also on the routes that build the INSERT before queueing it:
# data sent as native blocks, and INSERT ... SELECT. Here the table changes the type of `x` while the inserts wait.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ASYNC_SETTINGS="async_insert = 1, wait_for_async_insert = 0, async_insert_use_adaptive_busy_timeout = 0, async_insert_busy_timeout_min_ms = 600000, async_insert_busy_timeout_max_ms = 600000"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_expected_async;
    CREATE TABLE t_expected_async (x UInt8) ENGINE = MergeTree ORDER BY tuple();
"

echo 1 | $CLICKHOUSE_CLIENT -q "INSERT INTO t_expected_async SETTINGS $ASYNC_SETTINGS, insert_expected_column_types = {'x': 'UInt8'} FORMAT CSV"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_expected_async SELECT 2 SETTINGS $ASYNC_SETTINGS, insert_expected_column_types = {'x': 'UInt8'}"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_expected_async MODIFY COLUMN x UInt16 SETTINGS mutations_sync = 2"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_expected_async"
$CLICKHOUSE_CLIENT -q "SELECT 'queued as UInt8, flushed into UInt16', count() FROM t_expected_async"

# The same routes, with a requirement the table still meets at the flush.
echo 3 | $CLICKHOUSE_CLIENT -q "INSERT INTO t_expected_async SETTINGS $ASYNC_SETTINGS, insert_expected_column_types = {'x': 'UInt16'} FORMAT CSV"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_expected_async SELECT 4 SETTINGS $ASYNC_SETTINGS, insert_expected_column_types = {'x': 'UInt16'}"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_expected_async"
$CLICKHOUSE_CLIENT -q "SELECT 'queued and flushed as UInt16', groupArray(x) FROM (SELECT x FROM t_expected_async ORDER BY x)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_expected_async"
