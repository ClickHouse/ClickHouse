#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two ways select the size-stream String layout (a separate stream of cumulative byte offsets, the
# same layout Array uses, instead of a per-value length prefix):
#   * the native TCP protocol negotiates it automatically once both peers are recent enough, so the
#     clickhouse-client queries below use it without any extra option;
#   * the Native/Buffers *format* keeps the portable per-value layout by default and opts in through
#     output_format_native_write_string_with_size_stream / input_format_native_read_string_with_size_stream.
# A reader/writer mismatch would garble the values or throw.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512 (s String, a Array(String), n Nullable(String), m Map(String, String), tp Tuple(x String, y UInt64), lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512 SELECT concat('row', toString(number), repeat('x', number % 10)), arrayMap(i -> repeat('e', i), range(number % 4)), if(number % 3 = 0, NULL, toString(number)), map('k', repeat('v', number % 5)), (toString(number), number), 'lc' || toString(number % 3) FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04512"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_04512 ORDER BY tp.y LIMIT 4"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512"

# Sparse-serialized String columns keep working through the offsets wire format.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_sparse"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_sparse (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_sparse SELECT number, if(number % 100 = 0, 'rare' || toString(number), '') FROM numbers(10000)"
$CLICKHOUSE_CLIENT -q "SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_04512_sparse' AND column = 's' AND active"
$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(s != ''), min(s), max(s) FROM t_04512_sparse"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_sparse"

# The Native output format keeps the per-value varint layout by default and switches String columns to
# the offsets layout (UInt64 cumulative offsets, then the concatenated data) when the format setting is
# on. Unlike the per-value layout the offset is 8 bytes, so the two dumps differ even for one value.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_write_string_with_size_stream=1&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo

# Native bytes written with the size-stream layout are read back with the matching input setting.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt2"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt (s String, a Array(String), n Nullable(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt2 AS t_04512_rt"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_rt SELECT concat('v', toString(number), repeat('y', number % 7)), arrayMap(i -> toString(i), range(number % 3)), if(number % 2 = 0, NULL, toString(number)) FROM numbers(100)"

# Round-trip through the Native format at the default (per-value) layout and at the offsets layout, and
# through both the synchronous and the asynchronous insert paths (the async-insert queue keys batches by
# the query settings, so the two layouts never share a batch).
for size_stream in 0 1; do
    for insert_mode in "async_insert=0" "async_insert=1&wait_for_async_insert=1"; do
        $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_write_string_with_size_stream=${size_stream}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Native" > "${CLICKHOUSE_TMP}/04512_rt.native"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&input_format_native_read_string_with_size_stream=${size_stream}&${insert_mode}&query=INSERT+INTO+t_04512_rt2+FORMAT+Native" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.native"
        $CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
    done
done

# The Buffers format uses the same per-column representation as Native and the same format settings:
# per-value varints by default, the cumulative offsets layout when the setting is on (framing:
# num_columns, num_rows, per-column byte size, column bytes).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n'
echo
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_write_string_with_size_stream=1&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n'
echo

for size_stream in 0 1; do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_write_string_with_size_stream=${size_stream}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Buffers" > "${CLICKHOUSE_TMP}/04512_rt.buffers"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&input_format_native_read_string_with_size_stream=${size_stream}&async_insert=0&query=INSERT+INTO+t_04512_rt2+FORMAT+Buffers" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.buffers"
    $CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
done
rm -f "${CLICKHOUSE_TMP}/04512_rt.buffers"

# The Native format stays portable by default: a file written without the setting reads back without it.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('04512_${CLICKHOUSE_DATABASE}.native', 'Native', 's String') SELECT 'rev0 file' SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+*+FROM+file('04512_${CLICKHOUSE_DATABASE}.native',+'Native',+'s+String')"

# A corrupted offsets stream is reported as a regular error (INCORRECT_DATA), not as a logical error
# that aborts debug and sanitizer builds.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&output_format_native_write_string_with_size_stream=1&query=SELECT+'abcdefgh'+AS+s+FORMAT+Native" > "${CLICKHOUSE_TMP}/04512_rt.native"
python3 -c "
data = bytearray(open('${CLICKHOUSE_TMP}/04512_rt.native', 'rb').read())
data[-9] = 0x80  # the most significant byte of the UInt64 offset of the only value
open('${CLICKHOUSE_TMP}/04512_rt.native', 'wb').write(bytes(data))
"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&input_format_native_read_string_with_size_stream=1&async_insert=1&wait_for_async_insert=1&query=INSERT+INTO+t_04512_rt2+FORMAT+Native" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.native" | grep -oF "INCORRECT_DATA" | head -1

rm -f "${CLICKHOUSE_TMP}/04512_rt.native"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt2"
