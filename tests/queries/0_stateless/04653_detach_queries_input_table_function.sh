#!/usr/bin/env bash
# A query reading from `input` is fed by data the client pushes after the query, delivered through
# callbacks bound to the caller's connection. Such a query must never be detached: it has to run
# synchronously even when `allow_experimental_detach_queries` is enabled.
set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_input"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.t_input (x UInt64) ENGINE = Memory"

echo "=== Native: INSERT SELECT FROM input() runs synchronously ==="
# A detached query would answer with a `query_id` instead of inserting, and the rows would be lost.
echo -e "1\n2\n3" | $CLICKHOUSE_CLIENT --allow_experimental_detach_queries 1 --async_insert 0 \
    -q "INSERT INTO ${CLICKHOUSE_DATABASE}.t_input SELECT x FROM input('x UInt64') FORMAT TabSeparated"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_input"

echo "=== Native: a plain INSERT SELECT next to it is still detached ==="
QID="04653_${CLICKHOUSE_DATABASE}_detached"
RESP=$($CLICKHOUSE_CLIENT --query_id "$QID" --allow_experimental_detach_queries 1 --async_insert 0 \
    -q "INSERT INTO ${CLICKHOUSE_DATABASE}.t_input SELECT 4")
if [ -z "$RESP" ]; then
    echo "FAIL: expected a query_id for the detached INSERT SELECT"
    exit 1
fi
echo "<query_id>"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}.t_input"
