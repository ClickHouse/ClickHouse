#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query_id="$CLICKHOUSE_TEST_UNIQUE_NAME" --query="SELECT (SELECT max(number) FROM system.numbers) + 1 SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0" >/dev/null 2>&1 &
client_pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME'" | grep -qF '1' && break
    sleep 0.5
done

kill -INT $client_pid
wait $client_pid

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"

# Test cancellation of IN subquery with a MergeTree table
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t (col UInt64) ENGINE = MergeTree() ORDER BY col"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${CLICKHOUSE_TEST_UNIQUE_NAME}_t VALUES (rand()), (rand()), (rand())"

query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_in"
$CLICKHOUSE_CLIENT --query_id="$query_id" --query="SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_t WHERE col IN (SELECT max(rand()) FROM system.numbers) SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0" >/dev/null 2>&1 &
client_pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$query_id'" | grep -qF '1' && break
    sleep 0.5
done

kill -INT $client_pid
wait $client_pid

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$query_id' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"

$CLICKHOUSE_CLIENT --query "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
