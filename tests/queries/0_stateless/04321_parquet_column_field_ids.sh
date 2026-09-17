#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: Parquet read/write path and pyarrow are not available in the Fast test image.
#
# Verify that `output_format_parquet_column_field_ids` (Map override) and
# `output_format_parquet_auto_assign_field_ids` (Iceberg-style auto assign) write Parquet
# `field_id` metadata and don't break the value round-trip.
#
# Every `clickhouse-local` start costs many seconds under sanitizers, so the test batches all its
# queries into three processes: one for the accepted writes and their value round-trips, one
# `pyarrow` pass over every written file, and one `clickhouse-client --ignore-error` run for the
# rejected writes.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_TMP:?}"
mkdir -p "$WORKDIR"

# Files written by the accepted cases, in output order: `label` then path, pairwise.
FILES=(
    "explicit overrides"                       "$WORKDIR/04321_field_ids_custom.parquet"
    "auto-assign"                              "$WORKDIR/04321_field_ids_auto.parquet"
    "mixed override + auto-assign"             "$WORKDIR/04321_field_ids_mixed.parquet"
    "default (no settings)"                    "$WORKDIR/04321_field_ids_none.parquet"
    "nested auto-assign"                       "$WORKDIR/04321_field_ids_nested_auto.parquet"
    "nested overrides"                         "$WORKDIR/04321_field_ids_nested_overrides.parquet"
    "geo explicit override (Point as WKB)"     "$WORKDIR/04321_field_ids_geo_override.parquet"
    "geo auto-assign (Point as WKB)"           "$WORKDIR/04321_field_ids_geo_auto.parquet"
    "geo explicit override (MultiPoint as WKB)" "$WORKDIR/04321_field_ids_geo_multipoint_override.parquet"
    "geo auto-assign (MultiPoint as WKB)"      "$WORKDIR/04321_field_ids_geo_multipoint_auto.parquet"
    "max non-reserved id"                      "$WORKDIR/04321_field_ids_max_user_id.parquet"
)

# ClickHouse's settings parser only accepts string literals as Map values, so the `Int32` ids are
# written as strings here. The setting accepts both string-encoded ids and (programmatically) real
# integers.
${CLICKHOUSE_LOCAL} --output-format=TSV --query="
-- 1. Explicit per-column overrides.
SELECT '== explicit overrides ==';
INSERT INTO FUNCTION file('${FILES[1]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '10', 'b': '20', 'c': '30'};
SELECT a, b, c FROM file('${FILES[1]}');

-- 2. Auto-assign only.
SELECT '== auto-assign ==';
INSERT INTO FUNCTION file('${FILES[3]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
SELECT a, b, c FROM file('${FILES[3]}');

-- 3. Auto-assign + partial override: override wins for 'b', auto-assign fills 'a' and 'c'
--    skipping ids already claimed by the override.
SELECT '== mixed override + auto-assign ==';
INSERT INTO FUNCTION file('${FILES[5]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1,
         output_format_parquet_column_field_ids = {'b': '1'};
SELECT a, b, c FROM file('${FILES[5]}');

-- 4. Default path: writing still works, no field_id emitted.
SELECT '== default (no settings) ==';
INSERT INTO FUNCTION file('${FILES[7]}', 'Parquet')
SELECT 1::UInt32 AS a, 'hello'::String AS b, 42::Int64 AS c
SETTINGS engine_file_truncate_on_insert = 1;
SELECT a, b, c FROM file('${FILES[7]}');

-- 5. Nested types: auto-assign recursively walks Array.element, Tuple.<subfield>, Map.key/value
--    so the resulting Parquet schema is fully field_id-annotated.
SELECT '== nested auto-assign ==';
INSERT INTO FUNCTION file('${FILES[9]}', 'Parquet')
SELECT [1, 2, 3]::Array(UInt32) AS a, ('hi', 7)::Tuple(s String, i Int32) AS b, map('k', 1)::Map(String, UInt8) AS c
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;
SELECT a, b, c FROM file('${FILES[9]}');

-- 6. Nested overrides: dotted keys pin specific nested ids; auto-assign fills the gaps.
SELECT '== nested overrides ==';
INSERT INTO FUNCTION file('${FILES[11]}', 'Parquet')
SELECT [1, 2, 3]::Array(UInt32) AS a, ('hi', 7)::Tuple(s String, i Int32) AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1,
         output_format_parquet_column_field_ids = {'a.element': '100', 'b.s': '200'};
SELECT a, b FROM file('${FILES[11]}');

-- 7. Geo column with GeoParquet output (default output_format_parquet_geometadata = 1): the geo
--    type collapses to a single WKB String field, so field_ids apply to that one top-level field
--    only. The nested Tuple / Array shape of Point must not be enumerated (it is never written),
--    otherwise a top-level override is wrongly rejected as non-covering and auto-assign assigns ids
--    to fields that don't exist in the file.
INSERT INTO FUNCTION file('${FILES[13]}', 'Parquet')
SELECT (1.0, 2.0)::Point AS point
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'point': '7'};
INSERT INTO FUNCTION file('${FILES[15]}', 'Parquet')
SELECT (1.0, 2.0)::Point AS point
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;

--    MultiPoint collapses the same way (a single WKB String field), even though its ClickHouse type
--    is an Array of tuples - the field-id builder and the Iceberg validator must agree on that.
INSERT INTO FUNCTION file('${FILES[17]}', 'Parquet')
SELECT [(1.0, 2.0), (3.0, 4.0)]::MultiPoint AS mp
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'mp': '9'};
INSERT INTO FUNCTION file('${FILES[19]}', 'Parquet')
SELECT [(1.0, 2.0), (3.0, 4.0)]::MultiPoint AS mp
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;

-- 8. Iceberg reserves field ids above 2147483447 for its metadata fields; the largest non-reserved
--    id is still accepted.
INSERT INTO FUNCTION file('${FILES[21]}', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '2147483447'};

-- 9. The overrides are used by the Parquet output format only, and are parsed there, at the point
--    of use: FormatSettings are built for every input and output format of every query, so a
--    malformed value must not break a query that writes no Parquet at all - otherwise a user with
--    such a value in their profile could not run anything.
SELECT '== a malformed value only affects Parquet output ==';
SELECT 1 AS a
SETTINGS output_format_parquet_column_field_ids = {'a': 'oops'};
INSERT INTO FUNCTION file('$WORKDIR/04321_field_ids_tsv.tsv', 'TSV')
SELECT 2 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 'oops'};
SELECT * FROM file('$WORKDIR/04321_field_ids_tsv.tsv', 'TSV', 'a Int32')
SETTINGS output_format_parquet_column_field_ids = {'a': 'oops'};
"

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

# Rejected writes, batched into one `clickhouse-client` run: `--ignore-error` keeps the batch going
# after each failure and, unlike `clickhouse-local`, the client still prints the error. The marker
# rows are flushed to stdout when their query completes, before the next query's error reaches
# stderr, so the interleaved stream keeps the query order. The client reports every failure twice
# (the server exception and the query it failed on), so identical lines are collapsed per section.
# The server-side `file` function resolves paths under `user_files_path`.
ERR="${CLICKHOUSE_TEST_UNIQUE_NAME}_err"
${CLICKHOUSE_CLIENT} --ignore-error --output-format=TSV --query="
SELECT '== error: unknown column ==';
INSERT INTO FUNCTION file('${ERR}1.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'missing': '1'};

SELECT '== error: non-covering map without auto-assign ==';
INSERT INTO FUNCTION file('${ERR}2.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1'};

SELECT '== error: non-covering map skips nested field ==';
INSERT INTO FUNCTION file('${ERR}6.parquet', 'Parquet')
SELECT [1, 2, 3] AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1'};

SELECT '== error: duplicate id ==';
INSERT INTO FUNCTION file('${ERR}3.parquet', 'Parquet')
SELECT 1 AS a, 2 AS b
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1', 'b': '1'};

SELECT '== error: non-integer string value ==';
INSERT INTO FUNCTION file('${ERR}4.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': 'oops'};

SELECT '== error: negative id ==';
INSERT INTO FUNCTION file('${ERR}5.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '-1'};

-- Iceberg reserves field ids above 2147483447 for its metadata fields (e.g. _row_id); a data column
-- written with such an id would be silently ignored when the file is read as an Iceberg table, so
-- the override is rejected. 2147483540 is the reserved id of _row_id.
SELECT '== error: id in the Iceberg reserved range ==';
INSERT INTO FUNCTION file('${ERR}9.parquet', 'Parquet')
SELECT 1 AS a
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '2147483540'};

-- A top-level column whose name literally contains a '.' would flatten to the same dotted path as
-- a nested subfield of another column. Detected at field_id build time.
SELECT '== error: dotted top-level name collides with nested path (auto-assign) ==';
INSERT INTO FUNCTION file('${ERR}7.parquet', 'Parquet')
SELECT (1)::Tuple(b UInt8) AS a, 2::UInt8 AS \`a.b\`
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_auto_assign_field_ids = 1;

SELECT '== error: dotted top-level name collides with nested path (overrides) ==';
INSERT INTO FUNCTION file('${ERR}8.parquet', 'Parquet')
SELECT (1)::Tuple(b UInt8) AS a, 2::UInt8 AS \`a.b\`
SETTINGS engine_file_truncate_on_insert = 1,
         output_format_parquet_column_field_ids = {'a': '1', 'a.b': '2'};
" 2>&1 | grep -oE '^== error: .* ==$|BAD_ARGUMENTS|references unknown column|does not cover every output column|more than one column|is not an integer|must be non-negative|reserved by Iceberg|two output columns or nested fields flatten' \
  | awk '/^== /{delete seen} !seen[$0]++'
