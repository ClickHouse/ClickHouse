#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: Parquet read/write path and pyarrow are not available in the Fast test image.
#
# Verify that `output_format_parquet_column_field_ids` (Map override) and
# `output_format_parquet_auto_assign_field_ids` (Iceberg-style auto assign) write Parquet
# `field_id` metadata and don't break the value round-trip.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_TMP:?}"
mkdir -p "$WORKDIR"

dump_field_ids () {
    local path="$1"
    python3 -c "
import pyarrow as pa
import pyarrow.parquet as pq
schema = pq.read_schema('$path')

def field_id(field):
    if field.metadata:
        return field.metadata.get(b'PARQUET:field_id', b'-').decode()
    return '-'

def walk(field, prefix=''):
    full = f'{prefix}{field.name}' if prefix else field.name
    print(f'{full}\t{field_id(field)}')
    t = field.type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            walk(t.field(i), full + '.')
    elif pa.types.is_list(t) or pa.types.is_large_list(t):
        walk(t.value_field.with_name('element'), full + '.')
    elif pa.types.is_map(t):
        walk(t.key_field, full + '.')
        walk(t.item_field.with_name('value'), full + '.')

for field in schema:
    walk(field)
"
}

run_insert () {
    local label="$1"
    local file="$2"
    local query="$3"
    local select_columns="${4:-a, b, c}"
    echo "== $label =="
    ${CLICKHOUSE_LOCAL} --query="$query" --output-format=TSV
    echo "-- field_ids --"
    dump_field_ids "$file"
    echo "-- values --"
    ${CLICKHOUSE_LOCAL} --query="SELECT ${select_columns} FROM file('$file')"
}

# 1. Explicit per-column overrides. ClickHouse's settings parser only accepts string
#    literals as Map values, so the `Int32` ids are written as strings here. The
#    setting accepts both string-encoded ids and (programmatically) real integers.
F="$WORKDIR/04080_field_ids_custom.parquet"
run_insert "explicit overrides" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '10', 'b': '20', 'c': '30'};
"

# 2. Auto-assign only.
F="$WORKDIR/04080_field_ids_auto.parquet"
run_insert "auto-assign" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
"

# 3. Auto-assign + partial override: override wins for 'b', auto-assign fills 'a' and 'c'
#    skipping ids already claimed by the override.
F="$WORKDIR/04080_field_ids_mixed.parquet"
run_insert "mixed override + auto-assign" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1,
         output_format_parquet_column_field_ids = {'b': '1'};
"

# 4. Default path: writing still works, no `field_id` emitted.
F="$WORKDIR/04080_field_ids_none.parquet"
run_insert "default (no settings)" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1;
"

# 5. Nested types: auto-assign recursively walks Array.element, Tuple.<subfield>,
#    Map.key/value so the resulting Parquet schema is fully `field_id`-annotated.
F="$WORKDIR/04080_field_ids_nested_auto.parquet"
run_insert "nested auto-assign" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT [1, 2, 3]::Array(UInt32) AS a, ('hi', 7)::Tuple(s String, i Int32) AS b, map('k', 1)::Map(String, UInt8) AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
"

# 6. Nested overrides: dotted keys pin specific nested ids; auto-assign fills the gaps.
F="$WORKDIR/04080_field_ids_nested_overrides.parquet"
run_insert "nested overrides" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT [1, 2, 3]::Array(UInt32) AS a, ('hi', 7)::Tuple(s String, i Int32) AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1,
         output_format_parquet_column_field_ids = {'a.element': '100', 'b.s': '200'};
" "a, b"

echo "== error: unknown column =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err1.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'missing': '1'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|references unknown column' | sort -u

echo "== error: non-covering map without auto-assign =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err2.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|does not cover every output column' | sort -u

echo "== error: non-covering map skips nested field =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err6.parquet', 'Parquet')
SELECT [1, 2, 3] AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|does not cover every output column' | sort -u

echo "== error: duplicate id =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err3.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1', 'b': '1'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|more than one column' | sort -u

echo "== error: non-integer string value =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err4.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 'oops'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|is not an integer' | sort -u

echo "== error: negative id =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err5.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '-1'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|must be non-negative' | sort -u

# A top-level column whose name literally contains a '.' would flatten to the same dotted
# path as a nested subfield of another column. Detected at field_id build time.
echo "== error: dotted top-level name collides with nested path (auto-assign) =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err7.parquet', 'Parquet')
SELECT (1)::Tuple(b UInt8) AS a, 2::UInt8 AS \`a.b\`
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
" 2>&1 | grep -oE 'BAD_ARGUMENTS|two output columns or nested fields flatten' | sort -u

echo "== error: dotted top-level name collides with nested path (overrides) =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err8.parquet', 'Parquet')
SELECT (1)::Tuple(b UInt8) AS a, 2::UInt8 AS \`a.b\`
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1', 'a.b': '2'};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|two output columns or nested fields flatten' | sort -u
