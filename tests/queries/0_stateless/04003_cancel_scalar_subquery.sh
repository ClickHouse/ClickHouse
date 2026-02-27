#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query_id="$CLICKHOUSE_TEST_UNIQUE_NAME" --query="SELECT (SELECT max(number) FROM system.numbers) + 1" >/dev/null 2>&1 &
client_pid=$!

for _ in {0..60}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.processes WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME'" | grep -qF '1' && break
    sleep 0.5
done

kill -INT $client_pid
wait $client_pid
echo $?

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "SELECT exception FROM system.query_log WHERE query_id = '$CLICKHOUSE_TEST_UNIQUE_NAME' AND current_database = '$CLICKHOUSE_DATABASE'" | grep -oF "QUERY_WAS_CANCELLED"
