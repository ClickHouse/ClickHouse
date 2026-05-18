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
import pyarrow.parquet as pq
schema = pq.read_schema('$path')
for field in schema:
    print(f'{field.name}\t{field.metadata.get(b\"PARQUET:field_id\", b\"-\").decode()}' if field.metadata else f'{field.name}\t-')
"
}

run_insert () {
    local label="$1"
    local file="$2"
    local query="$3"
    echo "== $label =="
    ${CLICKHOUSE_LOCAL} --query="$query" --output-format=TSV
    echo "-- field_ids --"
    dump_field_ids "$file"
    echo "-- values --"
    ${CLICKHOUSE_LOCAL} --query="SELECT a, b, c FROM file('$file')"
}

# 1. Explicit per-column overrides.
F="$WORKDIR/04080_field_ids_custom.parquet"
run_insert "explicit overrides" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 10, 'b': 20, 'c': 30};
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
         output_format_parquet_column_field_ids = {'b': 1};
"

# 4. Default path: writing still works, no `field_id` emitted.
F="$WORKDIR/04080_field_ids_none.parquet"
run_insert "default (no settings)" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1;
"

# 5. Integer values may also be passed as quoted strings for compatibility.
F="$WORKDIR/04080_field_ids_string_values.parquet"
run_insert "string-encoded ids" "$F" "
INSERT INTO FUNCTION file('$F', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '7', 'b': '8', 'c': '9'};
"

echo "== error: unknown column =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err1.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'missing': 1};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|references unknown column' | sort -u

echo "== error: non-covering map without auto-assign =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err2.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 1};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|does not cover every output column' | sort -u

echo "== error: duplicate id =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04080_err3.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 1, 'b': 1};
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
         output_format_parquet_column_field_ids = {'a': -1};
" 2>&1 | grep -oE 'BAD_ARGUMENTS|must be non-negative' | sort -u
