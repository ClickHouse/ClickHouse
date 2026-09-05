#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

user="wrapper_hash_user_05055_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${user}"
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS d_definer_05055;
        DROP TABLE IF EXISTS m_definer_05055;
        DROP TABLE IF EXISTS d_invoker_05055;
        DROP TABLE IF EXISTS m_invoker_05055;
        DROP VIEW IF EXISTS v_definer_05055;
        DROP VIEW IF EXISTS v_invoker_05055;
        DROP TABLE IF EXISTS t_wrapper_hash_05055;
    "
}
trap cleanup EXIT

cleanup

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${user};
    CREATE TABLE t_wrapper_hash_05055 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_wrapper_hash_05055 VALUES (1);
    CREATE VIEW v_definer_05055 DEFINER = default SQL SECURITY DEFINER AS SELECT x FROM t_wrapper_hash_05055;
    CREATE VIEW v_invoker_05055 SQL SECURITY INVOKER AS SELECT x FROM t_wrapper_hash_05055;
    CREATE TABLE m_definer_05055 (x UInt64) ENGINE = Merge(currentDatabase(), '^v_definer_05055\$');
    CREATE TABLE m_invoker_05055 (x UInt64) ENGINE = Merge(currentDatabase(), '^v_invoker_05055\$');
    CREATE TABLE d_definer_05055 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), v_definer_05055);
    CREATE TABLE d_invoker_05055 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), v_invoker_05055);
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_definer_05055 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_definer_05055 TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d_definer_05055 TO ${user};
"

# The user can read the wrappers and the definer view behind them, but not the view's source table.
# A wrapper's recursive modification hash must not expose the source table's changes to this user,
# even though the wrapper's own storage is not a view.
$CLICKHOUSE_CLIENT --user "${user}" --query "
    SELECT name, modification_hash IS NULL
    FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('m_definer_05055', 'd_definer_05055')
    ORDER BY name
"

# A wrapper over an ordinary (\`INVOKER\`) view still reports a hash: the fail-close above is about
# crossing into a view that reads under an effective context, not about wrappers in general.
$CLICKHOUSE_CLIENT --query "
    SELECT name, modification_hash IS NOT NULL
    FROM system.tables
    WHERE database = '${CLICKHOUSE_DATABASE}' AND name IN ('m_invoker_05055', 'd_invoker_05055')
    ORDER BY name
"
