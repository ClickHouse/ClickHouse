#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -mq "
        DROP USER IF EXISTS with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
        DROP USER IF EXISTS without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
        DROP DATABASE IF EXISTS db_with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
    "
}
cleanup
trap cleanup EXIT

$CLICKHOUSE_CLIENT -mq "
    CREATE USER with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
    CREATE USER without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;

    GRANT CLUSTER, CREATE ON *.* TO with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
    GRANT CREATE ON *.* TO without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME;
"

echo "with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME"
$CLICKHOUSE_CLIENT --user "with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME" -q "CREATE DATABASE IF NOT EXISTS db_with_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME ON CLUSTER test_shard_localhost" >/dev/null
echo "without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME"
$CLICKHOUSE_CLIENT --user "without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME" -q "CREATE DATABASE IF NOT EXISTS db_without_on_cluster_$CLICKHOUSE_TEST_UNIQUE_NAME ON CLUSTER test_shard_localhost" |& {
    grep -m1 -F -o "Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. (ACCESS_DENIED)"
}
