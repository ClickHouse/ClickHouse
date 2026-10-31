#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: Parquet read/write path, Iceberg and pyarrow are not available in the Fast test image.
#
# Verify that `output_format_parquet_auto_assign_field_ids` writes Iceberg-style Parquet `field_id`
# metadata (sequential, starting at 1, in schema DFS order) and doesn't break the value round-trip.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_TMP:?}"
mkdir -p "$WORKDIR"

# Files written by the accepted cases, in output order: `label` then path, pairwise.
FILES=(
    "auto-assign"                         "$WORKDIR/04321_field_ids_auto.parquet"
    "default (no settings)"               "$WORKDIR/04321_field_ids_none.parquet"
    "nested auto-assign"                  "$WORKDIR/04321_field_ids_nested_auto.parquet"
    "geo auto-assign (Point as WKB)"      "$WORKDIR/04321_field_ids_geo_auto.parquet"
    "geo auto-assign (MultiPoint as WKB)" "$WORKDIR/04321_field_ids_geo_multipoint_auto.parquet"
)

ICEBERG_DIR="$WORKDIR/04321_field_ids_iceberg/"
rm -rf "$ICEBERG_DIR"

${CLICKHOUSE_LOCAL} --output-format=TSV --query="
SELECT '== auto-assign ==';
INSERT INTO FUNCTION file('${FILES[1]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
SELECT a, b, c FROM file('${FILES[1]}');

-- Default: writing still works, no field_id emitted.
SELECT '== default (no settings) ==';
INSERT INTO FUNCTION file('${FILES[3]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1;
SELECT a, b, c FROM file('${FILES[3]}');

-- Nested types: auto-assign recursively walks Array.element, Tuple.<subfield>, Map.key/value
-- so the resulting Parquet schema is fully field_id-annotated.
SELECT '== nested auto-assign ==';
INSERT INTO FUNCTION file('${FILES[5]}', 'Parquet')
SELECT [1, 2, 3]::Array(UInt32) AS a, ('hi', 7)::Tuple(s String, i Int32) AS b, map('k', 1)::Map(String, UInt8) AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
SELECT a, b, c FROM file('${FILES[5]}');

-- Geo columns with GeoParquet output (the default) collapse to a single WKB String field, so only
-- that top-level field gets a field_id.
INSERT INTO FUNCTION file('${FILES[7]}', 'Parquet')
SELECT (1.0, 2.0)::Point AS point
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
INSERT INTO FUNCTION file('${FILES[9]}', 'Parquet')
SELECT [(1.0, 2.0), (3.0, 4.0)]::MultiPoint AS mp
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;

"

# The field ids of an Iceberg table come from its metadata (the nested fields are numbered after the
# top-level columns), and the setting does not interfere with them.
${CLICKHOUSE_LOCAL} --output-format=TSV --query="
SELECT '== iceberg ==';
SET allow_insert_into_iceberg = 1, output_format_parquet_auto_assign_field_ids = 1;
CREATE TABLE t_iceberg (a Int64, t Tuple(x Int64, y String), b String) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet');
INSERT INTO t_iceberg VALUES (1, (2, 'y'), 'b');
SELECT * FROM t_iceberg;
" -- --user_files_path="$WORKDIR"
FILES+=("iceberg" "$(find "$ICEBERG_DIR" -name '*.parquet' | head -n1)")

# One pyarrow pass over every file written above: the field_id of every schema node, in DFS order.
python3 - "${FILES[@]}" <<'PY'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

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

args = sys.argv[1:]
for label, path in zip(args[0::2], args[1::2]):
    print(f'== field_ids: {label} ==')
    for field in pq.read_schema(path):
        walk(field)
PY

# A top-level column whose name literally contains a '.' flattens to the same dotted path as a
# nested subfield of another column, so the two cannot get distinct field ids.
echo "== error: dotted top-level name collides with nested path =="
${CLICKHOUSE_LOCAL} --query="
INSERT INTO FUNCTION file('$WORKDIR/04321_field_ids_err.parquet', 'Parquet')
SELECT tuple(1)::Tuple(b UInt8) AS a, 2::UInt8 AS \`a.b\`
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

rm -rf "$ICEBERG_DIR"
