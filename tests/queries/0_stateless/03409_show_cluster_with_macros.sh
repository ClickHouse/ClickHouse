#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Output of: SHOW CLUSTER '{default_cluster_macro}'"

# cluster,shard_num,shard_weight,internal_replication,replica_num,host_name,[host_address,]port,is_local,user,default_database[,errors_count,slowdowns_count,estimated_recovery_time,...]
$CLICKHOUSE_CLIENT -q "show cluster '{default_cluster_macro}'" | cut -f-6,8-10

echo "to match Output of: SHOW CLUSTER 'test_shard_localhost'"

$CLICKHOUSE_CLIENT -q "show cluster 'test_shard_localhost'" | cut -f-6,8-10
