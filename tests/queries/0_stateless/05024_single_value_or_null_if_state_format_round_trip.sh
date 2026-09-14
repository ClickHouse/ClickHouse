#!/usr/bin/env bash
set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=single_value_or_null_if_state_format_round_trip
STATE_FILE=$(mktemp "${CLICKHOUSE_TMP}/${TABLE}.XXXXXX")
trap 'rm -f -- "$STATE_FILE"; $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TABLE" >/dev/null 2>&1 || true' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE
    (
        state AggregateFunction(singleValueOrNullIf, UInt64, UInt8)
    )
    ENGINE = MergeTree
    ORDER BY tuple();
"

$CLICKHOUSE_CLIENT -q "SELECT toTypeName(singleValueOrNullIfState(number, 1)) FROM numbers(1)"

$CLICKHOUSE_CURL --fail "$CLICKHOUSE_URL" \
    --data-binary 'SELECT singleValueOrNullIfState(number, 1) AS state FROM numbers(2) FORMAT RowBinaryWithNamesAndTypes' \
    > "$STATE_FILE"

$CLICKHOUSE_CURL --fail \
    "${CLICKHOUSE_URL}&input_format_with_names_use_header=1&input_format_with_types_use_header=1&query=INSERT+INTO+${TABLE}+FORMAT+RowBinaryWithNamesAndTypes" \
    --data-binary "@$STATE_FILE" \
    > /dev/null

$CLICKHOUSE_CLIENT -q "SELECT singleValueOrNullIfMerge(state) FROM $TABLE"
