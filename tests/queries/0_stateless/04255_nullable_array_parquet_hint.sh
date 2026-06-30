#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression: Parquet/Arrow read with hint Nullable(Array(T)) must use the nullable
# decode path so list-level nulls are preserved in the outer null map.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

PARQUET_FILE="$TMP_DIR/nullable_list.parquet"
REQUIRED_PARQUET_FILE="$TMP_DIR/required_list.parquet"
ARROW_FILE="$TMP_DIR/nullable_list.arrow"
ORC_FILE="$TMP_DIR/nullable_list.orc"

python3 - "$PARQUET_FILE" "$REQUIRED_PARQUET_FILE" "$ARROW_FILE" <<'PYEOF'
import sys
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

parquet_path = sys.argv[1]
required_parquet_path = sys.argv[2]
arrow_path = sys.argv[3]
arr = pa.array([[1, 2], None, [], [3, 4]], type=pa.list_(pa.int32()))
table = pa.table({"arr": arr})
pq.write_table(table, parquet_path)

required_arr = pa.array([[1, 2], [], [3, 4]], type=pa.list_(pa.int32()))
required_schema = pa.schema([pa.field("arr", pa.list_(pa.int32()), nullable=False)])
required_table = pa.Table.from_arrays([required_arr], schema=required_schema)
pq.write_table(required_table, required_parquet_path)

with ipc.new_file(arrow_path, table.schema) as writer:
    writer.write_table(table)
PYEOF

$CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
SELECT arrayJoin([
    CAST([1, 2] AS Nullable(Array(Int32))),
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([] AS Nullable(Array(Int32))),
    CAST([3, 4] AS Nullable(Array(Int32)))
]) AS arr
FORMAT ORC
" > "$ORC_FILE"

result=$($CLICKHOUSE_LOCAL -q "
DESC file('$PARQUET_FILE', Parquet)
")
echo "$result" | grep -F 'arr' | grep -F 'Array(Nullable(Int32))' > /dev/null || {
    echo "Expected non-nullable Array(...) in Parquet schema inference with the setting disabled, got: $result"
    exit 1
}

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC file('$PARQUET_FILE', Parquet)
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in Parquet schema inference with the setting enabled, got: $result"
    exit 1
}

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC file('$PARQUET_FILE', Parquet)
SETTINGS schema_inference_make_columns_nullable = 0
")
echo "$result" | grep -E '^arr[[:space:]]+Array\(Int32\)[[:space:]]*$' > /dev/null || {
    echo "Expected non-nullable Array(...) in Parquet schema inference with nullable columns disabled, got: $result"
    exit 1
}

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC file('$REQUIRED_PARQUET_FILE', Parquet)
SETTINGS schema_inference_make_columns_nullable = 1
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in required Parquet list schema inference with nullable columns forced, got: $result"
    exit 1
}

$CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
SELECT throwIf(count() != 4 OR sum(isNull(arr)) != 1 OR countIf(length(arr) IS NULL) != 1 OR sum(length(arr)) != 4)
FROM file('$PARQUET_FILE', Parquet)
FORMAT Null
"

if $CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
SELECT * FROM file('$PARQUET_FILE', Parquet, 'arr Array(Int32)')
SETTINGS input_format_null_as_default = 0
FORMAT Null
" 2> "$TMP_DIR/non_nullable_array.err"; then
    echo "Expected non-nullable Array(...) Parquet read to reject NULL lists"
    exit 1
fi
grep -F 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' "$TMP_DIR/non_nullable_array.err" > /dev/null || {
    echo "Expected CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN for non-nullable Array(...) Parquet read, got:"
    cat "$TMP_DIR/non_nullable_array.err"
    exit 1
}

result=$($CLICKHOUSE_LOCAL -q "
DESC file('$ARROW_FILE', Arrow)
")
echo "$result" | grep -F 'arr' | grep -F 'Array(Nullable(Int32))' > /dev/null || {
    echo "Expected non-nullable Array(...) in schema inference with the setting disabled, got: $result"
    exit 1
}

if $CLICKHOUSE_LOCAL -q "
SELECT * FROM file('$ARROW_FILE', Arrow, 'arr Array(Int32)')
SETTINGS input_format_null_as_default = 0
FORMAT Null
" 2> "$TMP_DIR/non_nullable_array_arrow.err"; then
    echo "Expected non-nullable Array(...) Arrow read to reject NULL lists"
    exit 1
fi
grep -F 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' "$TMP_DIR/non_nullable_array_arrow.err" > /dev/null || {
    echo "Expected CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN for non-nullable Array(...) Arrow read, got:"
    cat "$TMP_DIR/non_nullable_array_arrow.err"
    exit 1
}

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC file('$ARROW_FILE', Arrow)
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in schema inference with the setting enabled, got: $result"
    exit 1
}

result=$($CLICKHOUSE_LOCAL -q "
DESC file('$ORC_FILE', ORC)
")
echo "$result" | grep -F 'arr' | grep -F 'Array(Nullable(Int32))' > /dev/null || {
    echo "Expected non-nullable Array(...) in ORC schema inference with the setting disabled, got: $result"
    exit 1
}

if $CLICKHOUSE_LOCAL -q "
SELECT * FROM file('$ORC_FILE', ORC, 'arr Array(Int32)')
SETTINGS input_format_null_as_default = 0
FORMAT Null
" 2> "$TMP_DIR/non_nullable_array_orc.err"; then
    echo "Expected non-nullable Array(...) ORC read to reject NULL lists"
    exit 1
fi
grep -F 'CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN' "$TMP_DIR/non_nullable_array_orc.err" > /dev/null || {
    echo "Expected CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN for non-nullable Array(...) ORC read, got:"
    cat "$TMP_DIR/non_nullable_array_orc.err"
    exit 1
}

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC file('$ORC_FILE', ORC)
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in ORC schema inference with the setting enabled, got: $result"
    exit 1
}

$CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
SELECT throwIf(count() != 4 OR sum(isNull(arr)) != 1 OR countIf(length(arr) IS NULL) != 1 OR sum(length(arr)) != 4)
FROM file('$PARQUET_FILE', Parquet, 'arr Nullable(Array(Int32))')
FORMAT Null
"

$CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
SELECT throwIf(count() != 4 OR sum(isNull(arr)) != 1 OR countIf(length(arr) IS NULL) != 1 OR sum(length(arr)) != 4)
FROM file('$ORC_FILE', ORC, 'arr Nullable(Array(Int32))')
FORMAT Null
"

result=$($CLICKHOUSE_LOCAL --allow_experimental_nullable_array_type=1 -q "
DESC format(JSONEachRow, '{\"arr\": null}
{\"arr\": [1,2]}') SETTINGS schema_inference_make_columns_nullable = 1
")
echo "$result" | grep -F 'arr' | grep -F 'Nullable(Array' > /dev/null || {
    echo "Expected Nullable(Array(...)) in schema inference, got: $result"
    exit 1
}
