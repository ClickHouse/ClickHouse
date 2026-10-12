#!/usr/bin/env bash
# A parameter with no bare literal form is printed with its type, so a state that crosses the
# Native protocol rebuilds the same parameter Field on the other side and is still accepted by a
# column declared from the same expression. `prefer_localhost_replica = 0` is what forces a real
# connection: a same-host, same-port shard is otherwise served locally, with no Native hop to cross.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS src_05239"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS dst_05239"

$CLICKHOUSE_CLIENT -q "CREATE TABLE src_05239 ENGINE = Memory AS
    SELECT groupArrayInsertAtState(toDecimal32(1.5, 1), 3)(x, i) AS s
    FROM (SELECT toDecimal32(2.5, 1) AS x, toUInt32(0) AS i)"
$CLICKHOUSE_CLIENT -q "CREATE TABLE dst_05239 ENGINE = Memory AS SELECT * FROM src_05239 WHERE 0"

$CLICKHOUSE_CLIENT -q "SELECT type FROM system.columns
    WHERE database = currentDatabase() AND table = 'dst_05239'"

$CLICKHOUSE_CLIENT -q "INSERT INTO dst_05239
    SELECT s FROM remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 'src_05239')
    SETTINGS prefer_localhost_replica = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}'"

$CLICKHOUSE_CLIENT -q "SELECT finalizeAggregation(s) FROM dst_05239"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# The two assertions above are identical whether the read was local or remote, so the hop itself is
# asserted here: a local shortcut dispatches no secondary query. The name is the carrier only while
# `output_format_native_encode_types_in_binary_format` is off, which is its default; with it on the
# type travels binary-encoded and this test no longer covers the printed name.
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.query_log
    WHERE is_initial_query = 0 AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}'
      AND current_database IN ['default', currentDatabase()]"

$CLICKHOUSE_CLIENT -q "DROP TABLE src_05239"
$CLICKHOUSE_CLIENT -q "DROP TABLE dst_05239"
