#!/usr/bin/env bash

# `system.tables.modification_hash` covers the whole table, so it must be `NULL` for a caller whose
# `SELECT` on the table is restricted by a non-trivial row policy: otherwise polling it would reveal
# changes to the rows the policy hides. A policy that is literally always true hides nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# Users and row policies are server-wide, so make the names unique per run.
user="user_05317_${CLICKHOUSE_DATABASE}"
policy="policy_05317_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_05317 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_05317 VALUES (1), (2);
    CREATE USER ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05317 TO ${user};
"

function show_hash()
{
    $CLICKHOUSE_CLIENT --user "${user}" -q "SELECT '$1', isNotNull(modification_hash) FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 't_05317'"
}

show_hash 'no policy'

$CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t_05317 USING x = 1 TO ${user}"
show_hash 'filtering policy'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT 'visible rows', count() FROM ${CLICKHOUSE_DATABASE}.t_05317"

$CLICKHOUSE_CLIENT -q "ALTER ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t_05317 USING 1"
show_hash 'always true policy'

$CLICKHOUSE_CLIENT -q "
    DROP ROW POLICY ${policy} ON ${CLICKHOUSE_DATABASE}.t_05317;
    DROP USER ${user};
"
