#!/usr/bin/env bash

CLICKHOUSE_CLIENT_OPT+="--allow_experimental_analyzer=0" # FIXME: analyzer is not supported yet


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_r1;
    drop table if exists data_r2;
    drop table if exists data_r3;
    drop table if exists data_r4;

    create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
    create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
    create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
    create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 50) settings cluster=1, cluster_replication_factor=2;
    select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;

    insert into data_r1 select number key, intDiv(number, 10) part, number value from numbers(10);

    system sync replica data_r1 cluster;
    system sync replica data_r2 cluster;
    system sync replica data_r3 cluster;
    system sync replica data_r4 cluster;
"

# break the replica
$CLICKHOUSE_CLIENT -q "system drop cluster replica data_r1"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '/tables/$CLICKHOUSE_DATABASE/data/replicas/1'"
# FIXME: should this node ("removed") moved into the replica_path?
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '/tables/$CLICKHOUSE_DATABASE/data/cluster/replicas/1'"

# TODO:
# - system stop replicated cluster;
# - system restore replica data_r1;
# - select throwIf(count() != 0) from data_r1 settings cluster_query_shards=0;
# - system sync replica data_r1 cluster;
# - select throwIf(count() == 5) from data_r1 settings cluster_query_shards=0;

$CLICKHOUSE_CLIENT -q "system restart replica data_r1"
# Ignore error about non existence replica in ReplicatedMergeTreeClusterReplica::fromCoordinator()
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal -q "system restore replica data_r1"

let i=0 attempts=100 # 10 sec
while [[ $i -lt $attempts ]]; do
    is_readonly=$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d @- <<<"select is_readonly from system.replicas where database = '$CLICKHOUSE_DATABASE' and table = 'data_r1'")
    if [[ $is_readonly = "0" ]]; then
        break
    fi
    (( ++i ))
    sleep 0.1
done
if [[ $is_readonly != "0" ]]; then
    echo "Replica is still readonly" >&2
    exit 1
fi

$CLICKHOUSE_CLIENT -nm -q "
system sync replica data_r1 cluster;
system sync replica data_r2 cluster;
system sync replica data_r3 cluster;
system sync replica data_r4 cluster;

system sync replica data_r1 cluster;

-- { echo }
select _table, count(), length(groupArrayDistinct(_partition_id)) from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select partition, count() from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select _table, count(), length(groupArrayDistinct(_partition_id)) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=1;
"
