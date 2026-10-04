#!/usr/bin/env bash
# `PARALLEL WITH` runs its subqueries as internal ones. A user `ATTACH DATABASE ... ENGINE = Cluster` or
# `ENGINE = Remote` wrapped in it is still a user query and must be validated eagerly, exactly like the plain one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_proxy"
db_other="${CLICKHOUSE_DATABASE}_other"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db; DROP DATABASE IF EXISTS $db_other"
}
cleanup

echo "--- Cluster with an unknown cluster name"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db_other ENGINE = Memory PARALLEL WITH ATTACH DATABASE $db ENGINE = Cluster('there_is_no_such_cluster', 'default')" 2>&1 | grep -o -m1 "CLUSTER_DOESNT_EXIST"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"
cleanup

echo "--- Cluster referring to itself"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db_other ENGINE = Memory PARALLEL WITH ATTACH DATABASE $db ENGINE = Cluster(test_shard_localhost, '$db')" 2>&1 | grep -o -m1 "INFINITE_LOOP"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"
cleanup

echo "--- Remote referring to itself"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db_other ENGINE = Memory PARALLEL WITH ATTACH DATABASE $db ENGINE = Remote('127.0.0.1', '$db', 'default', '')" 2>&1 | grep -o -m1 "INFINITE_LOOP"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"

cleanup
