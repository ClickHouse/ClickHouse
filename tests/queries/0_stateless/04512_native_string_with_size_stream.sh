#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native TCP protocol negotiates the size-stream String layout (a separate stream of cumulative
# byte offsets, the same layout Array uses, instead of a per-value length prefix) automatically once
# both peers are recent enough, so the clickhouse-client queries below exercise it transparently. The
# Native/Buffers *format* stays on the portable per-value layout; over HTTP a recent
# client_protocol_version selects the same revision-dependent encodings for Native output.

# clickhouse-client round-trips String inside nested types over the TCP protocol (size-stream layout).
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512 (s String, a Array(String), n Nullable(String), m Map(String, String), tp Tuple(x String, y UInt64), lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512 SELECT concat('row', toString(number), repeat('x', number % 10)), arrayMap(i -> repeat('e', i), range(number % 4)), if(number % 3 = 0, NULL, toString(number)), map('k', repeat('v', number % 5)), (toString(number), number), 'lc' || toString(number % 3) FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04512"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_04512 ORDER BY tp.y LIMIT 4"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512"

# Sparse-serialized String columns round-trip over the protocol.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_sparse"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_sparse (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_sparse SELECT number, if(number % 100 = 0, 'rare' || toString(number), '') FROM numbers(10000)"
$CLICKHOUSE_CLIENT -q "SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_04512_sparse' AND column = 's' AND active"
$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(s != ''), min(s), max(s) FROM t_04512_sparse"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_sparse"

# The Native HTTP output uses the portable per-value layout by default and the revision-dependent
# encodings (including the size-stream String layout) when a recent client_protocol_version is
# requested, so the two dumps differ.
native_default=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n')
native_revision=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54492&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n')
if [ "$native_default" != "$native_revision" ]; then echo "native dumps differ"; fi

# A per-value Native HTTP round-trip through both the synchronous and the asynchronous insert paths.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_rt2"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt (s String, a Array(String), n Nullable(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_rt2 AS t_04512_rt"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_rt SELECT concat('v', toString(number), repeat('y', number % 7)), arrayMap(i -> toString(i), range(number % 3)), if(number % 2 = 0, NULL, toString(number)) FROM numbers(100)"
for insert_mode in "async_insert=0" "async_insert=1&wait_for_async_insert=1"; do
    $CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Native" > "${CLICKHOUSE_TMP}/04512_rt.native"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${insert_mode}&query=INSERT+INTO+t_04512_rt2+FORMAT+Native" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.native"
    $CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
done
rm -f "${CLICKHOUSE_TMP}/04512_rt.native"

# Buffers is a plain format, not part of the protocol: it always uses the portable per-value layout,
# so client_protocol_version does not change its output.
buffers_default=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n')
buffers_revision=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54492&query=SELECT+'abc'+AS+s+FORMAT+Buffers" | od -An -v -tx1 | tr -d ' \n')
if [ "$buffers_default" = "$buffers_revision" ]; then echo "buffers dumps identical"; fi

$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE t_04512_rt2"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+*+FROM+t_04512_rt+ORDER+BY+s+FORMAT+Buffers" > "${CLICKHOUSE_TMP}/04512_rt.buffers"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_04512_rt2+FORMAT+Buffers" --data-binary @"${CLICKHOUSE_TMP}/04512_rt.buffers"
$CLICKHOUSE_CLIENT -q "SELECT count() = 100 AND groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) = (SELECT groupBitXor(cityHash64(s, arrayStringConcat(a), coalesce(n, ''))) FROM t_04512_rt) FROM t_04512_rt2"
rm -f "${CLICKHOUSE_TMP}/04512_rt.buffers"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_rt2"
