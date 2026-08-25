#!/usr/bin/env bash
# Tags: no-fasttest
# Test that PARALLEL WITH queries can be cancelled via Ctrl+C (SIGINT).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "CREATE TABLE t1 (col UInt64) ENGINE = MergeTree() ORDER BY col"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t2 (col UInt64) ENGINE = MergeTree() ORDER BY col"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query="
    INSERT INTO t1 SELECT number FROM system.numbers
    PARALLEL WITH
    INSERT INTO t2 SELECT number FROM system.numbers
    SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0
" >/dev/null 2>&1 &
client_pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$query_id'" | grep -qF '1' && break
    sleep 0.5
done

kill -INT $client_pid
wait $client_pid

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id' AND current_database = '$CLICKHOUSE_DATABASE' AND type != 'QueryStart'" | grep -oF "QUERY_WAS_CANCELLED"

$CLICKHOUSE_CLIENT --query "DROP TABLE t1"
$CLICKHOUSE_CLIENT --query "DROP TABLE t2"
