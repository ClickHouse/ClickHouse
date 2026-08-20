#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_FOREACH=single_value_or_null_foreach_state_format_round_trip
TABLE_MAP=single_value_or_null_map_state_format_round_trip
TABLE_MERGE_OR_DEFAULT=single_value_or_null_merge_or_default_state_format_round_trip
STATE_FILE_FOREACH=$(mktemp "${CLICKHOUSE_TMP}/${TABLE_FOREACH}.XXXXXX")
STATE_FILE_MAP=$(mktemp "${CLICKHOUSE_TMP}/${TABLE_MAP}.XXXXXX")
STATE_FILE_MERGE_OR_DEFAULT=$(mktemp "${CLICKHOUSE_TMP}/${TABLE_MERGE_OR_DEFAULT}.XXXXXX")
trap 'rm -f -- "$STATE_FILE_FOREACH" "$STATE_FILE_MAP" "$STATE_FILE_MERGE_OR_DEFAULT"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE_FOREACH; DROP TABLE IF EXISTS $TABLE_MAP; DROP TABLE IF EXISTS $TABLE_MERGE_OR_DEFAULT" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE_FOREACH;
    DROP TABLE IF EXISTS $TABLE_MAP;
    DROP TABLE IF EXISTS $TABLE_MERGE_OR_DEFAULT;

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

    CREATE TABLE $TABLE_MERGE_OR_DEFAULT
    (
        state AggregateFunction(1, singleValueOrNullMergeOrDefault, AggregateFunction(1, singleValueOrNull, UInt64))
    )
    ENGINE = MergeTree
    ORDER BY tuple();
"

$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullForEachState([toUInt64(42)])) FROM numbers(1)"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullMapState(map('key', toUInt64(42)))) FROM numbers(1)"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullOrDefaultState(toUInt64(42))) FROM numbers(1)"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullMergeOrDefaultState(s)) FROM (SELECT singleValueOrNullState(toUInt64(42)) AS s)"

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

$CLICKHOUSE_CURL --fail "$CLICKHOUSE_URL" \
    --data-binary "SELECT singleValueOrNullMergeOrDefaultState(s) AS state FROM (SELECT singleValueOrNullState(toUInt64(42)) AS s) FORMAT RowBinaryWithNamesAndTypes" \
    > "$STATE_FILE_MERGE_OR_DEFAULT"

$CLICKHOUSE_CURL --fail \
    "${CLICKHOUSE_URL}&input_format_with_names_use_header=1&input_format_with_types_use_header=1&query=INSERT+INTO+${TABLE_MERGE_OR_DEFAULT}+FORMAT+RowBinaryWithNamesAndTypes" \
    --data-binary "@$STATE_FILE_MERGE_OR_DEFAULT" \
    > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullForEachMerge(state) FROM $TABLE_FOREACH"
$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullMapMerge(state) FROM $TABLE_MAP"
$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullMergeOrDefaultMerge(state) FROM $TABLE_MERGE_OR_DEFAULT"
