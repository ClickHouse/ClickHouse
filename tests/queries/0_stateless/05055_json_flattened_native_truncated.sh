#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A flattened JSON/Dynamic Native block must be rejected when a stream ends before the declared row count.
# The data starts with rows that have no dynamic paths, so the Dynamic indexes stream starts with a run of
# NULL markers: truncated inside that run it demands no values at all and passes every per-type check.

DATA_FILE=$CLICKHOUSE_TMP/flattened_native_$CLICKHOUSE_DATABASE.bin

function check_truncated_blocks()
{
    local table=$1
    local query=$2

    $CLICKHOUSE_CLIENT --output_format_native_use_flattened_dynamic_and_json_serialization=1 -q "$query FORMAT Native" > "$DATA_FILE"

    local size
    size=$(stat -c%s "$DATA_FILE")
    local accepted=0
    local n
    for ((n = 1; n < size; n += 5)); do
        if head -c "$n" "$DATA_FILE" | $CLICKHOUSE_CLIENT -q "INSERT INTO $table FORMAT Native" 2>/dev/null; then
            accepted=$((accepted + 1))
        fi
    done

    echo "$table: accepted truncated blocks $accepted"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $table"

    $CLICKHOUSE_CLIENT -q "INSERT INTO $table FORMAT Native" < "$DATA_FILE"
    rm "$DATA_FILE"
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_flattened (json JSON(a UInt64, max_dynamic_paths=2)) ENGINE = Memory"
check_truncated_blocks t_json_flattened "
    SELECT (number < 6 ? '{\"a\":1}' : '{\"a\":2,\"b\":\"str' || toString(number) || '\",\"c\":' || toString(number) || '}')::JSON(a UInt64, max_dynamic_paths=2) AS json
    FROM numbers(12)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(json.a), countIf(json.b IS NULL), sum(json.c.:Int64) FROM t_json_flattened"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_json_array_flattened (arr Array(JSON)) ENGINE = Memory"
check_truncated_blocks t_json_array_flattened "
    SELECT arrayMap(x -> (number < 6 ? '{}' : '{\"a\":' || toString(x) || '}')::JSON, range(number % 3)) AS arr
    FROM numbers(12)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(length(arr)), sum(arraySum(arrayMap(x -> x.a::UInt64, arr))) FROM t_json_array_flattened"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_dynamic_flattened (dyn Dynamic) ENGINE = Memory"
check_truncated_blocks t_dynamic_flattened "
    SELECT json.a AS dyn
    FROM (SELECT (number < 6 ? '{}' : (number % 2 ? '{\"a\":' || toString(number) || '}' : '{\"a\":\"str' || toString(number) || '\"}'))::JSON AS json FROM numbers(12))"
$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(dyn IS NULL), sum(dyn.Int64), max(dyn.String) FROM t_dynamic_flattened"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_flattened"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_json_array_flattened"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_dynamic_flattened"
