#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
. "$CURDIR"/../shell_config.sh


function test()
{
    ENGINE=$1
    MAX_MEM=4096

    echo "Testing $ENGINE"

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS log";
    $CLICKHOUSE_CLIENT --query "CREATE TABLE log (x UInt64, y UInt64, z UInt64) ENGINE = $ENGINE";

    while true; do
        MAX_MEM=$((2 * $MAX_MEM))

        $CLICKHOUSE_CLIENT --query "INSERT INTO log SELECT number, number, number FROM numbers(1000000)" --max_memory_usage $MAX_MEM > "${CLICKHOUSE_TMP}"/insert_result 2>&1

        grep -o -F 'Memory limit' "${CLICKHOUSE_TMP}"/insert_result || cat "${CLICKHOUSE_TMP}"/insert_result

        $CLICKHOUSE_CLIENT --query "SELECT count(), sum(x + y + z) FROM log" > "${CLICKHOUSE_TMP}"/select_result 2>&1;

        grep -o -F 'File not found' "${CLICKHOUSE_TMP}"/select_result || cat "${CLICKHOUSE_TMP}"/select_result

        [[ $MAX_MEM -gt 200000000 ]] && break;
    done

    $CLICKHOUSE_CLIENT --query "DROP TABLE log";
}

test TinyLog | grep -v -P '^(Memory limit|0\t0|File not found|[1-9]000000\t)'
test StripeLog | grep -v -P '^(Memory limit|0\t0|File not found|[1-9]000000\t)'
test Log | grep -v -P '^(Memory limit|0\t0|File not found|[1-9]000000\t)'

rm "${CLICKHOUSE_TMP}/insert_result"
rm "${CLICKHOUSE_TMP}/select_result"
