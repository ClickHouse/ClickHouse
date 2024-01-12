#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "show clusters like 'test_shard%' limit 1"
# cluster,shard_num,shard_weight,internal_replication,replica_num,host_name,host_address,port,is_local,user,default_database[,errors_count,slowdowns_count,estimated_recovery_time]
# use a cluster with static IPv4
$CLICKHOUSE_CLIENT -q "show cluster 'test_cluster_one_shard_two_replicas'" | cut -f-10
