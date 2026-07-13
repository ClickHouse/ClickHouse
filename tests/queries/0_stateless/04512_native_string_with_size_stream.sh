#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Since DBMS_MIN_REVISION_WITH_STRING_WITH_SIZE_STREAM_SERIALIZATION, clickhouse-client and the
# server exchange String columns in the native protocol with a separate size stream. Every query
# below goes through that wire format in both directions; a reader/writer mismatch would garble
# the values or throw.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512 (s String, a Array(String), n Nullable(String), m Map(String, String), tp Tuple(x String, y UInt64), lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512 SELECT concat('row', toString(number), repeat('x', number % 10)), arrayMap(i -> repeat('e', i), range(number % 4)), if(number % 3 = 0, NULL, toString(number)), map('k', repeat('v', number % 5)), (toString(number), number), 'lc' || toString(number % 3) FROM numbers(1000)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04512"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_04512 ORDER BY tp.y LIMIT 4"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512"

# Sparse-serialized String columns keep working through the size-stream wire format.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04512_sparse"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04512_sparse (id UInt64, s String) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_04512_sparse SELECT number, if(number % 100 = 0, 'rare' || toString(number), '') FROM numbers(10000)"
$CLICKHOUSE_CLIENT -q "SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_04512_sparse' AND column = 's' AND active"
$CLICKHOUSE_CLIENT -q "SELECT count(), countIf(s != ''), min(s), max(s) FROM t_04512_sparse"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_04512_sparse"

# The Native output format keeps per-value varint sizes by default and switches String columns to
# the size-stream layout (UInt64 sizes, then concatenated data) when the requested protocol
# version is recent enough.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&client_protocol_version=54487&query=SELECT+'abc'+AS+s+FORMAT+Native" | od -An -v -tx1 | tr -d ' \n'
echo
