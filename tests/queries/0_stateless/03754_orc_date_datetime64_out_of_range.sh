#!/usr/bin/env bash
# Tags: no-fasttest

# test for ORC Date and DateTime64 out of range errors

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Date type range validation
echo "=== Test 1: Date type with value in valid range ==="
# 2060-01-01 is about 32873 days from epoch, within Date range [0, 65535]
$CLICKHOUSE_LOCAL -q "SELECT toDate32('2060-01-01') as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 Date" -q "SELECT * FROM table"

echo "=== Test 2: Date type with value outside valid range (too far in future) ==="
# 2200-01-01 is about 84006 days from epoch, which exceeds 65535 (Date max)
# read it as Date type, should throw error instead of silently truncating
$CLICKHOUSE_LOCAL -q "SELECT toDate32('2200-01-01') as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 Date" -q "SELECT * FROM table" 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1

echo "=== Test 3: Date type with negative value (before 1970-01-01) ==="
# date type cannot represent negative days, because it's UInt16
$CLICKHOUSE_LOCAL -q "SELECT toDate32('1960-01-01') as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 Date" -q "SELECT * FROM table" 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1

echo "=== Test 4: Date32 type should still work with extended range ==="
# reading as Date32 should work for all valid Date32 values
$CLICKHOUSE_LOCAL -q "SELECT toDate32('2200-01-01') as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 Date32" -q "SELECT * FROM table"
$CLICKHOUSE_LOCAL -q "SELECT toDate32('1960-01-01') as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 Date32" -q "SELECT * FROM table"

# DateTime64 overflow validation
echo "=== Test 5: DateTime64 with value in valid range ==="
# year 2200 is within DateTime64(9) range
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2200-01-01 00:00:00', 0) as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 DateTime64(9)" -q "SELECT * FROM table"

echo "=== Test 6: DateTime64 with extreme value causing overflow ==="
# DateTime64(0) can store year 2263, but when ORC reader converts to DateTime64(9)
# the multiplication seconds * 1e9 overflows Int64
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2263-01-01 00:00:00', 0) as c0 FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC --structure="c0 DateTime64(9)" -q "SELECT * FROM table" 2>&1 | grep -o "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE" | head -1
