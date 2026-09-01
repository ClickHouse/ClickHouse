#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The bulk Row codec runs on both the MergeTree path, where no format settings exist, and on
# `FORMAT Native`, which carries the settings of the query that reads the stream. The per-query
# `format_binary_max_string_size` bound on the row payload has to apply there too.

NATIVE_FILE="$CLICKHOUSE_TMP"/05067_row.native

$CLICKHOUSE_LOCAL --allow_experimental_row_type=1 \
    -q "SELECT tuple(1, 2)::Row(a UInt8, b UInt8) AS r FORMAT Native" > "$NATIVE_FILE"

$CLICKHOUSE_LOCAL --allow_experimental_row_type=1 --format_binary_max_string_size=1 \
    -q "SELECT * FROM file('$NATIVE_FILE', Native)" 2>&1 | grep -c -F "TOO_LARGE_STRING_SIZE"

$CLICKHOUSE_LOCAL --allow_experimental_row_type=1 \
    -q "SELECT * FROM file('$NATIVE_FILE', Native)"

rm -f "$NATIVE_FILE"
