#!/usr/bin/env bash
# Native TCP insert path: INSERT FORMAT with external data + RETURNING.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o errexit

$CLICKHOUSE_CLIENT --async_insert=0 --query "DROP TABLE IF EXISTS t_insert_returning_native"
$CLICKHOUSE_CLIENT --async_insert=0 --query "CREATE TABLE t_insert_returning_native (id UInt64, name String) ENGINE = Memory"

echo -e '1\tfoo' | $CLICKHOUSE_CLIENT --async_insert=0 --query "INSERT INTO t_insert_returning_native (id, name) RETURNING (SELECT id, name FROM t_insert_returning_native WHERE id = 1 ORDER BY id) FORMAT TabSeparated"

$CLICKHOUSE_CLIENT --async_insert=0 --query "SELECT id, name FROM t_insert_returning_native ORDER BY id"

$CLICKHOUSE_CLIENT --async_insert=0 --query "DROP TABLE t_insert_returning_native"
