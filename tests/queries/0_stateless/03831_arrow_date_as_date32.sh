#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that Date columns are output as Arrow date32 type, not uint16.
# https://github.com/ClickHouse/ClickHouse/issues/96834

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.data

# Schema inference should show Date32 (Arrow date32 maps to ClickHouse Date32)
echo "Arrow schema inference:"
$CLICKHOUSE_LOCAL -q "SELECT toDate('2024-01-15') AS d FORMAT Arrow" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Arrow')"

echo "ArrowStream schema inference:"
$CLICKHOUSE_LOCAL -q "SELECT toDate('2024-01-15') AS d FORMAT ArrowStream" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'ArrowStream')"

# Verify values are preserved through roundtrip
echo "Arrow roundtrip:"
$CLICKHOUSE_LOCAL -q "SELECT toDate('2024-01-15') AS d, toDate('2020-12-31') AS e FORMAT Arrow" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Arrow')"

echo "ArrowStream roundtrip:"
$CLICKHOUSE_LOCAL -q "SELECT toDate('2024-01-15') AS d, toDate('2020-12-31') AS e FORMAT ArrowStream" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'ArrowStream')"

# With output_format_arrow_date_as_uint16=1, schema inference should show UInt16
echo "Arrow schema inference (uint16 compat):"
$CLICKHOUSE_LOCAL --output_format_arrow_date_as_uint16=1 -q "SELECT toDate('2024-01-15') AS d FORMAT Arrow" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'Arrow')"

echo "ArrowStream schema inference (uint16 compat):"
$CLICKHOUSE_LOCAL --output_format_arrow_date_as_uint16=1 -q "SELECT toDate('2024-01-15') AS d FORMAT ArrowStream" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', 'ArrowStream')"

# Verify values are preserved with uint16 compat mode
echo "Arrow roundtrip (uint16 compat):"
$CLICKHOUSE_LOCAL --output_format_arrow_date_as_uint16=1 -q "SELECT toDate('2024-01-15') AS d, toDate('2020-12-31') AS e FORMAT Arrow" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "select * from file('$DATA_FILE', 'Arrow')"

rm "$DATA_FILE"
