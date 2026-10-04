#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The scalar subquery is executed while the server is still analysing the query, so progress and profile
# events reach the client before the INSERT header block and must be drained, not rejected. Reading the row
# from stdin keeps the sample block requested whatever `async_insert` and
# `send_table_structure_on_insert_with_inline_data` are randomized to.

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (v UInt64) ENGINE = MergeTree ORDER BY tuple()"
echo '42' | ${CLICKHOUSE_CLIENT} -q "INSERT INTO t SELECT x + (SELECT sum(number) FROM numbers(10)) FROM input('x UInt64') FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t"

# An interserver INSERT waits for the same header block on the initiator. The shard folds the CHECK
# constraint's scalar subquery while analysing the bare INSERT the initiator sent it, so the same two
# packets arrive first; an initiator that rejects them reports UNEXPECTED_PACKET_FROM_SERVER and hides
# the shard's own diagnostic. `prefer_localhost_replica = 0` is what makes the write go over the wire.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_shard (v UInt64, CONSTRAINT c CHECK v >= (SELECT 1)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), t_shard) SETTINGS prefer_localhost_replica = 0, distributed_foreground_insert = 1 VALUES (1)" 2>&1 |
    grep -oF -e UNEXPECTED_PACKET_FROM_SERVER -e UNKNOWN_IDENTIFIER | head -n 1
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_shard"
