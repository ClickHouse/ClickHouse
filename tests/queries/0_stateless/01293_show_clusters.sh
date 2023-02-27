#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "show clusters like 'test_shard%' limit 1"
# cluster,shard_num,shard_weight,replica_num,host_name,host_address,port,is_local,user,default_database[,errors_count,slowdowns_count,estimated_recovery_time]
$CLICKHOUSE_CLIENT -q "show cluster 'test_shard_localhost'" | cut -f-10
