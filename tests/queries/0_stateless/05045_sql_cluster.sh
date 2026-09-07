#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLUSTER="test_sql_cluster_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP CLUSTER IF EXISTS ${CLUSTER}"

$CLICKHOUSE_CLIENT -q "
CREATE CLUSTER ${CLUSTER} (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000)
    )
)";

$CLICKHOUSE_CLIENT -q "SELECT shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = '${CLUSTER}' ORDER BY shard_num, replica_num FORMAT TSV";

$CLICKHOUSE_CLIENT -q "
ALTER CLUSTER ${CLUSTER} (
    user = 'default',
    SHARD (
        REPLICA (host = '127.0.0.1', port = 9000),
        REPLICA (host = '127.0.0.2', port = 9000)
    )
)";

$CLICKHOUSE_CLIENT -q "SELECT shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = '${CLUSTER}' ORDER BY shard_num, replica_num FORMAT TSV";

$CLICKHOUSE_CLIENT -q "DROP CLUSTER ${CLUSTER}";

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.clusters WHERE cluster = '${CLUSTER}'";

$CLICKHOUSE_CLIENT -q "
CREATE CLUSTER ${CLUSTER} (
    user = 'default',
    SHARD (
        host = '127.0.0.1',
        port = 9000
    )
)";

$CLICKHOUSE_CLIENT -q "SELECT shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = '${CLUSTER}' ORDER BY shard_num, replica_num FORMAT TSV";

$CLICKHOUSE_CLIENT -q "DROP CLUSTER ${CLUSTER}";

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.clusters WHERE cluster = '${CLUSTER}'";
