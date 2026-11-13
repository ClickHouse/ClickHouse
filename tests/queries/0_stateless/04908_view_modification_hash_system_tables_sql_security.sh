#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

user="view_hash_user_04908_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
    $CLICKHOUSE_CLIENT -q "DROP VIEW IF EXISTS v_view_hash_04908"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_view_hash_04908"
}
trap cleanup EXIT

cleanup

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${user};
    CREATE TABLE t_view_hash_04908 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_view_hash_04908 VALUES (1);
    CREATE VIEW v_view_hash_04908 DEFINER = default SQL SECURITY DEFINER AS SELECT x FROM t_view_hash_04908;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_view_hash_04908 TO ${user};
"

# The user can read the view but not its source. A definer view's recursive modification hash would
# otherwise expose source-table changes to this user through `system.tables`.
$CLICKHOUSE_CLIENT --user ${user} --query "
    SELECT modification_hash IS NULL
    FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'v_view_hash_04908'
"
