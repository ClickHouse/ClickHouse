#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Since DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION, clickhouse-client and the
# server exchange String columns in the native protocol with a separate stream that carries the
# cumulative byte offsets as-is (the same layout Array uses for its offsets). Every query below goes
# through that wire format in both directions; a reader/writer mismatch would garble the values or throw.

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

# The Native output format keeps per-value varint sizes by default and switches String columns to
# the offsets layout (UInt64 cumulative offsets, then concatenated data) when the requested protocol
# version is recent enough. For a single value the offset equals its size, so the bytes match here.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54488&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo

# Native bytes produced with an explicit client_protocol_version can be inserted back through the
# Native input format at the same protocol version (the reader side honors it too).
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt2"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt (s String, a Array(String), n Nullable(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt2 AS t_04512_rt"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_rt SELECT concat('v', toString(number), repeat('y', number % 7)), arrayMap(i -> toString(i), range(number % 3)), if(number % 2 = 0, NULL, toString(number)) FROM numbers(100)"

# Both the synchronous and the asynchronous insert paths parse the data at the protocol version
# of the inserting connection (the async-insert queue keys batches by it and restores it on the
# flush context).
for version in "" "&client_protocol_version=54488"; do
    for insert_mode in "async_insert=0" "async_insert=1&wait_for_async_insert=1"; do
        $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}${version}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Native" > "${CLICKHOUSE_TMP}/04512_rt.native"
        ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}${version}&${insert_mode}&query=INSERT+INTO+t_04512_rt2+FORMAT+Native" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.native"
        $CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
    done
done
# The Buffers format uses the same per-column representation as Native and follows the same
# revision-dependent encodings: per-value varints by default, the cumulative offsets layout at a
# raised protocol version (framing: num_columns, num_rows, per-column byte size, column bytes).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n'
echo
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54488&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n'
echo

for version in "" "&client_protocol_version=54488"; do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}${version}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Buffers" > "${CLICKHOUSE_TMP}/04512_rt.buffers"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}${version}&async_insert=0&query=INSERT+INTO+t_04512_rt2+FORMAT+Buffers" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.buffers"
    $CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
done
rm -f "${CLICKHOUSE_TMP}/04512_rt.buffers"

# A raised protocol version on the request applies to the INSERT body only: a file written at
# revision 0 still parses at revision 0 inside such a request (file(), s3(), url() reads).
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('04512_${CLICKHOUSE_DATABASE}.native', 'Native', 's String') SELECT 'rev0 file' SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54488&query=SELECT+*+FROM+file('04512_${CLICKHOUSE_DATABASE}.native',+'Native',+'s+String')"

# A corrupted offsets stream is reported as a regular error (INCORRECT_DATA), not as a logical error
# that aborts debug and sanitizer builds.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54488&query=SELECT+'abcdefgh'+AS+s+FORMAT+Native" > "${CLICKHOUSE_TMP}/04512_rt.native"
python3 -c "
data = bytearray(open('${CLICKHOUSE_TMP}/04512_rt.native', 'rb').read())
data[-9] = 0x80  # the most significant byte of the UInt64 offset of the only value
open('${CLICKHOUSE_TMP}/04512_rt.native', 'wb').write(bytes(data))
"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54488&async_insert=1&wait_for_async_insert=1&query=INSERT+INTO+t_04512_rt2+FORMAT+Native" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.native" | grep -oF "INCORRECT_DATA" | head -1

rm -f "${CLICKHOUSE_TMP}/04512_rt.native"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt2"
