#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA="$CURDIR/data_puffin"
OUT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$OUT"
rm -f "${OUT:?}"/*.puffin

echo "--- same deletion vector as Spark: blob bytes are identical ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/spark.puffin', Puffin)
SELECT * FROM file('$DATA/spark_deletion_vector.puffin', Puffin)
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

echo "--- one blob per referenced data file, ordered by the data file ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/two.puffin', Puffin)
SELECT * FROM values('referenced_data_file String, deleted_rows Array(UInt64)',
    ('data/b.parquet', [200, 100]), ('data/a.parquet', [2]), ('data/b.parquet', [100, 300]), ('data/a.parquet', [1]))
"
$CLICKHOUSE_LOCAL -q "
SELECT blob_type, snapshot_id, sequence_number, fields, offset, length, properties FROM file('$OUT/two.puffin', PuffinMetadata) ORDER BY offset
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/two.puffin', Puffin) ORDER BY referenced_data_file
"

echo "--- a file with several deletion vectors is copied as is ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/two_copy.puffin', Puffin)
SELECT * FROM file('$OUT/two.puffin', Puffin)
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT arraySort(groupArray((referenced_data_file, deleted_rows))) FROM file('$OUT/two_copy.puffin', Puffin))
    = (SELECT arraySort(groupArray((referenced_data_file, deleted_rows))) FROM file('$OUT/two.puffin', Puffin)),
    (SELECT arraySort(groupArray((offset, length, properties))) FROM file('$OUT/two_copy.puffin', PuffinMetadata))
    = (SELECT arraySort(groupArray((offset, length, properties))) FROM file('$OUT/two.puffin', PuffinMetadata))
"

echo "--- one position per row: unsorted duplicates, positions above 2^32, boundary position ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/rows.puffin', Puffin)
SELECT 'data/a.parquet' AS referenced_data_file, arrayJoin([5, 2, 5, 4294967301, 12884901888, 0, 9223372030412324864])::UInt64 AS position
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
SELECT 'data/large.parquet', arrayJoin([
    9223372030412324864, 9223372030412324863, 9223372026117357567, 9223372026117357568,
    17179869183, 12884901888, 8589934592, 8589934591, 6442450944,
    4294967297, 4294967296, 4294967295, 2147483648, 2147483647
])::UInt64 AS position
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
SELECT 'data/large_int64.parquet', arrayJoin([toInt64(9223372030412324864), toInt64(17179869183), toInt64(4294967295), toInt64(2147483648)]) AS position
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT deleted_rows FROM file('$OUT/large_int64.puffin', Puffin))
    = [2147483648, 4294967295, 17179869183, 9223372030412324864]
"

echo "--- empty arrays: empty deletion vector ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/empty_dv.puffin', Puffin)
SELECT * FROM values('referenced_data_file String, deleted_rows Array(UInt64)', ('data/c.parquet', []), ('data/c.parquet', []))
"
$CLICKHOUSE_LOCAL -q "
SELECT offset, length, properties FROM file('$OUT/empty_dv.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/empty_dv.puffin', Puffin)
"

echo "--- no rows: no blobs ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/no_blobs.puffin', Puffin)
SELECT toString(number), number FROM numbers(0)
"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$OUT/no_blobs.puffin', PuffinMetadata)
"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$OUT/no_blobs.puffin', Puffin)
"

echo "--- dense range round trip ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/dense.puffin', Puffin)
SELECT * FROM file('$DATA/dense_range_100k.puffin', Puffin)
"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT (referenced_data_file, deleted_rows) FROM file('$OUT/dense.puffin', Puffin))
    = (SELECT (referenced_data_file, deleted_rows) FROM file('$DATA/dense_range_100k.puffin', Puffin)),
    (SELECT length FROM file('$OUT/dense.puffin', PuffinMetadata)) = (SELECT length FROM file('$DATA/dense_range_100k.puffin', PuffinMetadata))
"

echo "--- other column types ---"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/uint8.puffin', Puffin)
SELECT 'data/e.parquet', [3, 1, 2] AS deleted_rows
"
$CLICKHOUSE_LOCAL -q "
INSERT INTO FUNCTION file('$OUT/int64.puffin', Puffin)
SELECT toLowCardinality('data/f.parquet'), arrayJoin([toInt64(7), toInt64(4294967296)]) AS position
"
$CLICKHOUSE_LOCAL -q "
SELECT referenced_data_file, deleted_rows FROM file('$OUT/{uint8,int64}.puffin', Puffin) ORDER BY referenced_data_file
"

echo "--- written to stdout, read from stdin ---"
$CLICKHOUSE_LOCAL -q "
SELECT 'data/h.parquet', arrayJoin([10, 20, 30])::UInt64 AS position FORMAT Puffin
" | $CLICKHOUSE_LOCAL --input-format Puffin \
    --structure 'referenced_data_file String, deleted_rows Array(UInt64)' \
    -q "SELECT referenced_data_file, deleted_rows FROM table"

echo "--- errors ---"
$CLICKHOUSE_LOCAL -q "SELECT [1]::Array(UInt64) AS deleted_rows FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Puffin output format requires two columns: the referenced data file (a string) and deleted row positions (an integer or an array of integers), got deleted_rows" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 1 AS referenced_data_file, [1]::Array(UInt64) AS deleted_rows FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "requires the first column with the referenced data file to be a string, got UInt8" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'data/x.parquet', '1' AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "requires the second column with deleted row positions to be an integer or an array of integers, got String" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'data/x.parquet', [1]::Array(Nullable(UInt64)) AS deleted_rows FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "requires the second column with deleted row positions to be an integer or an array of integers, got Array(Nullable(UInt64))" | head -1
$CLICKHOUSE_LOCAL -q "SELECT '', 1::UInt64 AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Referenced data file of a deletion vector must not be empty" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'data/x.parquet', -1::Int64 AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Deleted row position -1 is negative" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'data/x.parquet', 9223372030412324865::UInt64 AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Deleted row position 9223372030412324865 exceeds the maximum deletion vector position" | head -1
$CLICKHOUSE_LOCAL -q "SELECT 'data/x.parquet', 9223372036854775807::Int64 AS position FORMAT Puffin" 2>&1 >/dev/null \
    | grep -o "Deleted row position 9223372036854775807 exceeds the maximum deletion vector position" | head -1

rm -rf "${OUT:?}"
