#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Can be slow and resource intensive

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function test_func()
{
    ENGINE=$1
    MAX_MEM=4096

    echo "Testing $ENGINE"

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS log";
    $CLICKHOUSE_CLIENT --query "CREATE TABLE log (x UInt64, y UInt64, z UInt64) ENGINE = $ENGINE";

    while true; do
        MAX_MEM=$((2 * $MAX_MEM))

        $CLICKHOUSE_CLIENT --query "INSERT INTO log SELECT number, number, number FROM numbers(1000000)" --max_memory_usage $MAX_MEM > "${CLICKHOUSE_TMP}"/insert_result 2>&1
        RES=$?

        grep -o -F 'Memory limit' "${CLICKHOUSE_TMP}"/insert_result || cat "${CLICKHOUSE_TMP}"/insert_result

        $CLICKHOUSE_CLIENT --query "SELECT count(), sum(x + y + z) FROM log" > "${CLICKHOUSE_TMP}"/select_result 2>&1;

        cat "${CLICKHOUSE_TMP}"/select_result

        { [[ $RES -eq 0 ]] || [[ $MAX_MEM -gt 200000000 ]]; } && break;
    done

    $CLICKHOUSE_CLIENT --query "DROP TABLE log";
}

test_func TinyLog | grep -v -P '^(Memory limit|0\t0|[1-9]000000\t)'
test_func StripeLog | grep -v -P '^(Memory limit|0\t0|[1-9]000000\t)'
test_func Log | grep -v -P '^(Memory limit|0\t0|[1-9]000000\t)'

rm "${CLICKHOUSE_TMP}/insert_result"
rm "${CLICKHOUSE_TMP}/select_result"
