#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: creates and drops a global SQL-managed cluster

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

user="u_sql_cluster_rbac_${CLICKHOUSE_DATABASE}"
cluster="c_sql_cluster_rbac_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "
        DROP CLUSTER IF EXISTS ${cluster};
        DROP USER IF EXISTS ${user};
    "
}

trap cleanup EXIT
cleanup

check()
{
    local query="$1"
    local output
    if output="$($CLICKHOUSE_CLIENT --user "${user}" -q "${query}" 2>&1)"
    then
        echo "OK"
    elif echo "${output}" | grep -q "ACCESS_DENIED"
    then
        echo "ACCESS_DENIED"
    else
        echo "${output}"
    fi
}

create_stmt="CREATE CLUSTER ${cluster} (SHARD (REPLICA (host = '127.0.0.1', port = 9000)))"
alter_stmt="ALTER CLUSTER ${cluster} (SHARD (REPLICA (host = '127.0.0.1', port = 9000), REPLICA (host = '127.0.0.2', port = 9000)))"
drop_stmt="DROP CLUSTER ${cluster}"

$CLICKHOUSE_CLIENT -q "CREATE USER ${user}"

echo "-- no grants: every statement is denied"
check "${create_stmt}"
check "${alter_stmt}"
check "${drop_stmt}"

echo "-- CREATE CLUSTER alone: CREATE ok; ALTER/DROP denied"
$CLICKHOUSE_CLIENT -q "GRANT CREATE CLUSTER ON *.* TO ${user}"
check "${create_stmt}"
check "${alter_stmt}"
check "${drop_stmt}"

echo "-- CREATE privilege group must not unlock ALTER CLUSTER"
$CLICKHOUSE_CLIENT -q "
    REVOKE CREATE CLUSTER ON *.* FROM ${user};
    GRANT CREATE ON *.* TO ${user};
"
check "${alter_stmt}"
$CLICKHOUSE_CLIENT -q "REVOKE CREATE ON *.* FROM ${user}"

echo "-- ALTER CLUSTER alone: ALTER ok; DROP denied"
$CLICKHOUSE_CLIENT -q "GRANT ALTER CLUSTER ON *.* TO ${user}"
check "${alter_stmt}"
check "${drop_stmt}"

echo "-- DROP CLUSTER alone: DROP ok"
$CLICKHOUSE_CLIENT -q "
    REVOKE ALTER CLUSTER ON *.* FROM ${user};
    GRANT DROP CLUSTER ON *.* TO ${user};
"
check "${drop_stmt}"

echo "-- recreating requires CREATE again"
check "${create_stmt}"
$CLICKHOUSE_CLIENT -q "GRANT CREATE CLUSTER ON *.* TO ${user}"
check "${create_stmt}"
$CLICKHOUSE_CLIENT -q "DROP CLUSTER IF EXISTS ${cluster}"
