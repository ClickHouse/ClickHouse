#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP CLUSTER IF EXISTS test_sql_cluster_simple"

$CLICKHOUSE_CLIENT -q "
CREATE CLUSTER test_sql_cluster_simple (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
)";

$CLICKHOUSE_CLIENT -q "SELECT cluster, shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = 'test_sql_cluster_simple' ORDER BY shard_num, replica_num FORMAT TSV";

$CLICKHOUSE_CLIENT -q "
ALTER CLUSTER test_sql_cluster_simple (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    )
)";

$CLICKHOUSE_CLIENT -q "SELECT cluster, shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = 'test_sql_cluster_simple' ORDER BY shard_num, replica_num FORMAT TSV";

$CLICKHOUSE_CLIENT -q "DROP CLUSTER test_sql_cluster_simple";

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.clusters WHERE cluster = 'test_sql_cluster_simple'";
