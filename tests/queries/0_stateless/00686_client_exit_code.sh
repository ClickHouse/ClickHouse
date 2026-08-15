#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

echo "INSERT INTO test FORMAT CSV" | ${CLICKHOUSE_CLIENT} 2>/dev/null
echo $?

# A second interrupt abandons the query and force-exits the client with 128 + SIGINT.
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_sigint"
${CLICKHOUSE_CLIENT} --query_id="$query_id" --query "SELECT sleepEachRow(1) FROM numbers(5) SETTINGS function_sleep_max_microseconds_per_block = 10000000, max_block_size = 5" > /dev/null 2>&1 &
client_pid=$!

for _ in {0..300}; do
    [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" = "1" ] && break
    sleep 0.1
done

kill -INT $client_pid
sleep 0.5
kill -INT $client_pid
wait $client_pid
echo $?

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '$query_id' FORMAT Null"
