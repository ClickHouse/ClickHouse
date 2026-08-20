#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_FOREACH=single_value_or_null_foreach_state_format_round_trip
TABLE_MAP=single_value_or_null_map_state_format_round_trip
STATE_FILE_FOREACH=$(mktemp "${CLICKHOUSE_TMP}/${TABLE_FOREACH}.XXXXXX")
STATE_FILE_MAP=$(mktemp "${CLICKHOUSE_TMP}/${TABLE_MAP}.XXXXXX")
trap 'rm -f -- "$STATE_FILE_FOREACH" "$STATE_FILE_MAP"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE_FOREACH; DROP TABLE IF EXISTS $TABLE_MAP" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE_FOREACH;
    DROP TABLE IF EXISTS $TABLE_MAP;

    CREATE TABLE $TABLE_FOREACH
    (
        state AggregateFunction(singleValueOrNullForEach, Array(UInt64))
    )
    ENGINE = MergeTree
    ORDER BY tuple();

    CREATE TABLE $TABLE_MAP
    (
        state AggregateFunction(singleValueOrNullMap, Map(String, UInt64))
    )
    ENGINE = MergeTree
    ORDER BY tuple();
"

$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullForEachState([toUInt64(42)])) FROM numbers(1)"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullMapState(map('key', toUInt64(42)))) FROM numbers(1)"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullOrDefaultState(toUInt64(42))) FROM numbers(1)"

$CLICKHOUSE_CURL --fail "$CLICKHOUSE_URL" \
    --data-binary "SELECT singleValueOrNullForEachState([toUInt64(42)]) AS state FROM numbers(1) FORMAT RowBinaryWithNamesAndTypes" \
    > "$STATE_FILE_FOREACH"

$CLICKHOUSE_CURL --fail \
    "${CLICKHOUSE_URL}&input_format_with_names_use_header=1&input_format_with_types_use_header=1&query=INSERT+INTO+${TABLE_FOREACH}+FORMAT+RowBinaryWithNamesAndTypes" \
    --data-binary "@$STATE_FILE_FOREACH" \
    > /dev/null

$CLICKHOUSE_CURL --fail "$CLICKHOUSE_URL" \
    --data-binary "SELECT singleValueOrNullMapState(map('key', toUInt64(42))) AS state FROM numbers(1) FORMAT RowBinaryWithNamesAndTypes" \
    > "$STATE_FILE_MAP"

$CLICKHOUSE_CURL --fail \
    "${CLICKHOUSE_URL}&input_format_with_names_use_header=1&input_format_with_types_use_header=1&query=INSERT+INTO+${TABLE_MAP}+FORMAT+RowBinaryWithNamesAndTypes" \
    --data-binary "@$STATE_FILE_MAP" \
    > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullForEachMerge(state) FROM $TABLE_FOREACH"
$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullMapMerge(state) FROM $TABLE_MAP"
