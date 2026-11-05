#!/usr/bin/env bash
# Tags: no-darwin
# no-darwin: `system.parts_columns.filenames` of a wide part is empty on macOS ARM, https://github.com/ClickHouse/ClickHouse/issues/127841

# What a part of a JSON column with SHARED paths is made of, and the wire: the three typed shared data substreams and
# their files, no per-path stream, the object serialization versions, Native and RowBinary with and without the binary
# type encoding, remote(), packed part storage, sizes, codecs, and the engines that store the type outside parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SHARED="JSON(SHARED a UInt64, SHARED b String, SHARED c Array(UInt8))"
PLAIN="JSON(a UInt64, b String, c Array(UInt8))"
ROW="concat('{\"a\":', toString(number * 10), ',\"b\":\"', repeat('x', number % 7), '\",\"c\":', toString(range(number % 4)), if(number % 2 = 0, concat(',\"d\":', toString(number)), ''), '}')"
WIDE="min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 4, min_compress_block_size = 1, replace_long_file_name_to_hash = 0"
COMPACT="min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 4, replace_long_file_name_to_hash = 0"

echo "-- the substreams and files of the typed shared data, no per-path stream"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_w; DROP TABLE IF EXISTS t_c; DROP TABLE IF EXISTS t_plain;
CREATE TABLE t_w (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE;
CREATE TABLE t_c (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $COMPACT;
CREATE TABLE t_plain (x UInt64, json $PLAIN) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE;
INSERT INTO t_w SELECT number, $ROW FROM numbers(20);
INSERT INTO t_c SELECT * FROM t_w;
INSERT INTO t_plain SELECT * FROM t_w;
SELECT table, arraySort(arrayFilter(s -> s LIKE '%typed_shared_data%', substreams)), arraySort(arrayFilter(f -> f LIKE '%typed_shared_data%', filenames)), arrayExists(s -> s IN ('json.a', 'json.b', 'json.c'), substreams), arrayExists(s -> s IN ('a', 'b', 'c'), subcolumns.names)
    FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_w', 't_c', 't_plain') AND column = 'json' AND active ORDER BY table;
SELECT table, column_data_compressed_bytes > 0, column_data_uncompressed_bytes > 0, column_marks_bytes > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_w' AND column = 'json' AND active;
SELECT table, arraySort(arrayFilter(x -> x.1 IN ('a', 'b', 'c'), arrayZip(subcolumns.names, arrayMap(bytes -> bytes > 0, subcolumns.data_compressed_bytes), arrayMap(bytes -> bytes > 0, subcolumns.data_uncompressed_bytes))))
    FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_w', 't_plain') AND column = 'json' AND active ORDER BY table;
SELECT table, data_compressed_bytes > 0 FROM system.columns WHERE database = currentDatabase() AND table = 't_w' AND name = 'json';
CHECK TABLE t_w SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_c SETTINGS check_query_single_value_result = 1;
"

echo "-- the object serialization versions write the same typed shared data"
q=""
for v in v1 v2 v3; do
    q+="DROP TABLE IF EXISTS t_$v;
    CREATE TABLE t_$v (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE, object_serialization_version = '$v';
    INSERT INTO t_$v SELECT number, $ROW FROM numbers(20);
    SELECT '$v', arraySort(arrayFilter(s -> s LIKE '%typed_shared_data%', substreams)) = (SELECT arraySort(arrayFilter(s -> s LIKE '%typed_shared_data%', substreams)) FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_w' AND column = 'json' AND active),
        (SELECT arraySort(groupArray((x, toString(json)))) FROM t_$v) = (SELECT arraySort(groupArray((x, toString(json)))) FROM t_plain)
        FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_$v' AND column = 'json' AND active;
    DROP TABLE t_$v;
"
done
$CLICKHOUSE_CLIENT -n -q "$q"

echo "-- Native through clickhouse-local, the RowBinary bytes of the SHARED and the plain type, a Native dump inserted back"
$CLICKHOUSE_CLIENT -q "SELECT json FROM t_w ORDER BY x FORMAT Native" | $CLICKHOUSE_LOCAL --input-format Native --structure "json $SHARED" -q "SELECT toTypeName(json), count(), sum(json.a), sum(length(json.c)) FROM table"
$CLICKHOUSE_CLIENT -q "SELECT json FROM t_w ORDER BY x FORMAT Native" | $CLICKHOUSE_LOCAL --input-format Native --structure "json $PLAIN" -q "SELECT 'read as the plain type', toTypeName(json), count(), sum(json.a) FROM table"
SHARED_MD5=$($CLICKHOUSE_CLIENT -q "SELECT json FROM t_w ORDER BY x FORMAT RowBinary" | md5sum | cut -d' ' -f1)
PLAIN_MD5=$($CLICKHOUSE_CLIENT -q "SELECT json FROM t_plain ORDER BY x FORMAT RowBinary" | md5sum | cut -d' ' -f1)
[ "$SHARED_MD5" = "$PLAIN_MD5" ] && echo "RowBinary bytes equal: 1" || echo "RowBinary bytes equal: 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_back; CREATE TABLE t_back (json $SHARED) ENGINE = MergeTree ORDER BY tuple() SETTINGS $WIDE"
$CLICKHOUSE_CLIENT -q "SELECT json FROM t_w ORDER BY x FORMAT Native" | $CLICKHOUSE_CLIENT -q "INSERT INTO t_back FORMAT Native"
$CLICKHOUSE_CLIENT -q "SELECT 'inserted back', count(), sum(json.a), sum(json.c.size0) FROM t_back"

echo "-- the binary type encoding: a SHARED path carries a flag byte, the version is checked on decoding"
# The server formats the output: the client would re-encode the blocks with its own settings.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_binary_encode_types_in_binary_format=1" -d "SELECT json FROM ${CLICKHOUSE_DATABASE}.t_w ORDER BY x FORMAT RowBinaryWithNamesAndTypes" | $CLICKHOUSE_LOCAL --input-format RowBinaryWithNamesAndTypes --input_format_binary_decode_types_in_binary_format 1 -q "SELECT 'RowBinary', toTypeName(json), count(), sum(json.a) FROM table"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_encode_types_in_binary_format=1" -d "SELECT json FROM ${CLICKHOUSE_DATABASE}.t_w ORDER BY x FORMAT Native" | $CLICKHOUSE_LOCAL --input-format Native --input_format_native_decode_types_in_binary_format 1 -q "SELECT 'Native', toTypeName(json), count(), sum(json.a) FROM table"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_encode_types_in_binary_format=1" -d "SELECT CAST(concat('{\"a\":', toString(number), ',\"b\":\"b\",\"d\":1}'), 'JSON(a UInt64, SHARED b String, max_dynamic_paths = 3)') AS json FROM numbers(5) FORMAT Native" | $CLICKHOUSE_LOCAL --input-format Native --input_format_native_decode_types_in_binary_format 1 -q "SELECT 'mixed', toTypeName(json), count(), sum(json.a), any(json.b) FROM table"
# A hand-built header: column json with a JSON type of version 2, then of version 1 with an unknown flag bit on the path.
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(RowBinaryWithNamesAndTypes, unhex('01046a736f6e3002')) SETTINGS input_format_binary_decode_types_in_binary_format = 1" 2>&1 | grep -o "Unexpected version of JSON type binary encoding" | head -1
$CLICKHOUSE_CLIENT -q "SELECT * FROM format(RowBinaryWithNamesAndTypes, unhex('01046a736f6e300180082001016102040000')) SETTINGS input_format_binary_decode_types_in_binary_format = 1" 2>&1 | grep -o "Unknown flags [0-9]* of typed path '[a-z]*'" | head -1
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(json), count() FROM format(RowBinaryWithNamesAndTypes, unhex('01046a736f6e300180082001016101040000')) SETTINGS input_format_binary_decode_types_in_binary_format = 1"

echo "-- remote()"
$CLICKHOUSE_CLIENT -n -q "
SELECT 'remote', (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0))) FROM remote('127.0.0.1', currentDatabase(), t_w)) = (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0))) FROM t_w),
    (SELECT toTypeName(json) FROM remote('127.0.0.1', currentDatabase(), t_w) LIMIT 1), (SELECT toTypeName(json.a) FROM remote('127.0.0.1', currentDatabase(), t_w) LIMIT 1);
"

echo "-- packed part storage"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_packed;
CREATE TABLE t_packed (x UInt64, json $SHARED) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE, min_bytes_for_full_part_storage = 1000000000;
INSERT INTO t_packed SELECT number, $ROW FROM numbers(60);
SELECT part_storage_type, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_packed' AND active;
SELECT 'packed', (SELECT arraySort(groupArray((x, toString(json)))) FROM t_packed SETTINGS max_threads = 1, max_block_size = 5) = (SELECT arraySort(groupArray((x, toString(json)))) FROM (SELECT number AS x, CAST($ROW, '$PLAIN') AS json FROM numbers(60))),
    (SELECT arraySort(groupArray((x, json.a, json.c.size0))) FROM t_packed WHERE x >= 9 AND x < 16 SETTINGS max_threads = 1) = (SELECT arraySort(groupArray((number, number * 10, number % 4))) FROM numbers(9, 7)),
    (SELECT arraySort(groupArray((x, json.a, toString(json)))) FROM t_packed WHERE x >= 13 AND x < 50 SETTINGS max_threads = 1, max_block_size = 5) = (SELECT arraySort(groupArray((x, json.a, toString(json)))) FROM (SELECT number AS x, CAST($ROW, '$PLAIN') AS json FROM numbers(13, 37)));
CHECK TABLE t_packed SETTINGS check_query_single_value_result = 1;
DROP TABLE t_packed;
"

echo "-- codecs"
q=""
for codec in "CODEC(ZSTD(1))" "CODEC(NONE)" "CODEC(LZ4HC(3))"; do
    q+="DROP TABLE IF EXISTS t_codec;
    CREATE TABLE t_codec (x UInt64, json $SHARED $codec) ENGINE = MergeTree ORDER BY x SETTINGS $WIDE;
    INSERT INTO t_codec SELECT number, $ROW FROM numbers(20);
    SELECT '$codec', (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0))) FROM t_codec SETTINGS max_threads = 1, max_block_size = 7) = (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0))) FROM t_plain);
    CHECK TABLE t_codec SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_codec;
"
done
$CLICKHOUSE_CLIENT -n -q "$q"

echo "-- the engines that store the type outside parts: Memory, Buffer, Merge, file()"
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS t_mem; DROP TABLE IF EXISTS t_buf; DROP TABLE IF EXISTS t_merge;
CREATE TABLE t_mem (x UInt64, json $SHARED) ENGINE = Memory;
INSERT INTO t_mem SELECT * FROM t_w;
SELECT 'Memory', (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0, json.^a))) FROM t_mem) = (SELECT arraySort(groupArray((x, toString(json), json.a, json.c.size0, json.^a))) FROM t_plain);
CREATE TABLE t_buf (x UInt64, json $SHARED) ENGINE = Buffer(currentDatabase(), t_c, 1, 10, 100, 10000, 1000000, 10000000, 100000000);
INSERT INTO t_buf SELECT number, $ROW FROM numbers(20, 20);
SELECT 'Buffer', count(), sum(json.a) FROM t_buf;
OPTIMIZE TABLE t_buf;
SELECT 'Buffer flushed', count(), sum(json.a), sum(json.c.size0) FROM t_c;
CREATE TABLE t_merge (x UInt64, json $SHARED) ENGINE = Merge(currentDatabase(), '^t_(w|c)$');
SELECT 'Merge', count(), sum(json.a), sum(json.c.size0), uniqExact(_table) FROM t_merge;
"
# Join and the Log family reject JSON columns altogether, like the plain type.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_join (x UInt64, json $SHARED) ENGINE = Join(ANY, LEFT, x)" 2>&1 | grep -o "Code: [0-9]*" | head -1
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}.native', Native, 'x UInt64, json $SHARED') SELECT x, json FROM t_w ORDER BY x SETTINGS engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "SELECT 'file()', count(), sum(json.a), sum(json.c.size0), toTypeName(json) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.native', Native, 'x UInt64, json $SHARED') GROUP BY toTypeName(json)"
$CLICKHOUSE_CLIENT -q "SELECT 'file() inferred', toTypeName(json) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.native', Native) LIMIT 1"

$CLICKHOUSE_CLIENT -n -q "DROP TABLE t_w; DROP TABLE t_c; DROP TABLE t_plain; DROP TABLE t_back; DROP TABLE t_mem; DROP TABLE t_buf; DROP TABLE t_merge;"
