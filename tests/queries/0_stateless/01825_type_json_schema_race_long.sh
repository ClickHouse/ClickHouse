#!/usr/bin/env bash
# Tags: no-fasttest, long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_race"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_race (data Object('json')) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

function test_case()
{
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_json_race"

    echo '{"data": {"k1": 1, "k2": 2}}' | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_race FORMAT JSONEachRow"

    pids=()
    for _ in {1..5}; do
        $CLICKHOUSE_CLIENT -q "SELECT * FROM t_json_race WHERE 0 IN (SELECT sleep(0.05)) FORMAT Null" &
        pids+=($!)
    done

    echo '{"data": {"k1": "str", "k2": "str1"}}' | $CLICKHOUSE_CLIENT -q "INSERT INTO t_json_race FORMAT JSONEachRow" &

    for pid in "${pids[@]}"; do
        wait "$pid" || exit 1
    done
}

for _ in {1..30}; do test_case; done

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_json_race"
echo OK
