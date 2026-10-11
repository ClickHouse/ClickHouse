#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$OUT"
rm -f "${OUT:?}"/*.puffin

SPARK_DATA_FILE="/tmp/clickhouse_spark_puffin_9eiw_i48/default/spark_puffin_source/data/00000-0-15387e93-06e3-48bb-ab87-ea4bfedb0c1d-0-00001.parquet"

echo "--- same deletion vector as Spark: blob bytes are identical ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/spark.puffin', Puffin)
SELECT deleted_rows FROM file('$DATA/spark_deletion_vector.puffin', Puffin)
SETTINGS output_format_puffin_referenced_data_file = '$SPARK_DATA_FILE'
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT hex(substring(raw, 5, 58)) FROM file('$OUT/spark.puffin', RawBLOB, 'raw String'))
    = (SELECT hex(substring(raw, 5, 58)) FROM file('$DATA/spark_deletion_vector.puffin', RawBLOB, 'raw String'))
"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, snapshot_id, sequence_number, fields, offset, length, compression_codec, properties
FROM file('$OUT/spark.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/spark.puffin', Puffin)
"

echo "--- one position per row: unsorted duplicates, positions above 2^32, boundary position ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/rows.puffin', Puffin)
SELECT arrayJoin([5, 2, 5, 4294967301, 12884901888, 0, 9223372030412324864])::UInt64 AS position
SETTINGS output_format_puffin_referenced_data_file = 'data/a.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT offset, length, properties FROM file('$OUT/rows.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/rows.puffin', Puffin)
"

echo "--- large positions around 32-bit key and low part boundaries ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/large.puffin', Puffin)
SELECT arrayJoin([
    9223372030412324864, 9223372030412324863, 9223372026117357567, 9223372026117357568,
    17179869183, 12884901888, 8589934592, 8589934591, 6442450944,
    4294967297, 4294967296, 4294967295, 2147483648, 2147483647
])::UInt64 AS position
SETTINGS output_format_puffin_referenced_data_file = 'data/large.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT properties['cardinality'] FROM file('$OUT/large.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT arrayJoin(deleted_rows) AS position, bitShiftRight(position, 32) AS key, bitAnd(position, 0xFFFFFFFF) AS low
FROM file('$OUT/large.puffin', Puffin)
"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/large_int64.puffin', Puffin)
SELECT arrayJoin([toInt64(9223372030412324864), toInt64(17179869183), toInt64(4294967295), toInt64(2147483648)]) AS position
SETTINGS output_format_puffin_referenced_data_file = 'data/large_int64.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT deleted_rows FROM file('$OUT/large_int64.puffin', Puffin))
    = [2147483648, 4294967295, 17179869183, 9223372030412324864]
"

echo "--- arrays from several rows are merged ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/arrays.puffin', Puffin)
SELECT * FROM values('deleted_rows Array(UInt64)', ([3, 1]), ([]), ([2, 3]))
SETTINGS output_format_puffin_referenced_data_file = 'data/b.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/arrays.puffin', Puffin)
"

echo "--- no rows: empty deletion vector ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/empty.puffin', Puffin)
SELECT number FROM numbers(0)
SETTINGS output_format_puffin_referenced_data_file = 'data/c.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT offset, length, properties FROM file('$OUT/empty.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/empty.puffin', Puffin)
"

echo "--- dense range round trip ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/dense.puffin', Puffin)
SELECT deleted_rows FROM file('$DATA/dense_range_100k.puffin', Puffin)
SETTINGS output_format_puffin_referenced_data_file = 'data/d.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT deleted_rows FROM file('$OUT/dense.puffin', Puffin)) = (SELECT deleted_rows FROM file('$DATA/dense_range_100k.puffin', Puffin)),
    (SELECT length FROM file('$OUT/dense.puffin', PuffinMetadata)) = (SELECT length FROM file('$DATA/dense_range_100k.puffin', PuffinMetadata))
"

echo "--- other integer types ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/uint8.puffin', Puffin)
SELECT [3, 1, 2] AS deleted_rows
SETTINGS output_format_puffin_referenced_data_file = 'data/e.parquet'
"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/int64.puffin', Puffin)
SELECT arrayJoin([toInt64(7), toInt64(4294967296)]) AS position
SETTINGS output_format_puffin_referenced_data_file = 'data/f.parquet'
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/{uint8,int64}.puffin', Puffin) ORDER BY referenced_data_file
"

echo "--- blob metadata settings ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/settings.puffin', Puffin)
SELECT arrayJoin([1, 2])::UInt64 AS position
SETTINGS output_format_puffin_referenced_data_file = 'data/g.parquet', output_format_puffin_snapshot_id = 8143950620832403401, output_format_puffin_sequence_number = 7, output_format_puffin_field_ids = '2147483645, 2147483546'
"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, snapshot_id, sequence_number, fields, properties FROM file('$OUT/settings.puffin', PuffinMetadata)
"

echo "--- written to stdout, read from stdin ---"
$CLICKHOUSE_LOCAL -q "
SELECT arrayJoin([10, 20, 30])::UInt64 AS position FORMAT Puffin
SETTINGS output_format_puffin_referenced_data_file = 'data/h.parquet'
" | $CLICKHOUSE_LOCAL --input-format Puffin \
    --structure 'referenced_data_file String, deleted_rows Array(UInt64)' \
    -q "SELECT referenced_data_file, deleted_rows FROM table"

echo "--- errors ---"
$CLICKHOUSE_LOCAL -q "SELECT 1::UInt64 AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Setting output_format_puffin_referenced_data_file must be set" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'x' AS referenced_data_file, [1]::Array(UInt64) AS deleted_rows FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "Puffin output format requires exactly one column" | head -1
$CLICKHOUSE_LOCAL -q "SELECT '1' AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "requires a column of deleted row positions of an integer type or an array of integers, got String" | head -1
$CLICKHOUSE_LOCAL -q "SELECT [1]::Array(Nullable(UInt64)) AS deleted_rows FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "requires a column of deleted row positions of an integer type or an array of integers, got Array(Nullable(UInt64))" | head -1
$CLICKHOUSE_LOCAL -q "SELECT -1::Int64 AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "Deleted row position -1 is negative" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 9223372030412324865::UInt64 AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "Deleted row position 9223372030412324865 exceeds the maximum deletion vector position" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 9223372036854775807::Int64 AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet'" 2>&1 >/dev/null \
    | grep -o "Deleted row position 9223372036854775807 exceeds the maximum deletion vector position" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 1::UInt64 AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet', output_format_puffin_field_ids = ''" 2>&1 >/dev/null \
    | grep -o "Setting output_format_puffin_field_ids must be a comma-separated list of Int32 field ids" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 1::UInt64 AS position FORMAT Puffin SETTINGS output_format_puffin_referenced_data_file = 'data/x.parquet', output_format_puffin_field_ids = '1,x'" 2>&1 >/dev/null \
    | grep -o "Setting output_format_puffin_field_ids must be a comma-separated list of Int32 field ids" | head -1

rm -rf "${OUT:?}"
