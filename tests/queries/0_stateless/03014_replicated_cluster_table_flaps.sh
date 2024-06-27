#!/usr/bin/env bash
# Tags: no-fasttest

CLICKHOUSE_CLIENT_OPT+="--allow_experimental_analyzer=0" # FIXME: analyzer is not supported yet

# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function sync_cluster()
{
    local table
    # Note, we need to sync data_r1 at the end since we are using this table
    # for query to cluster_partitions and we need to sync the cluster
    # partitions map after all other replicas had been synced. But, to simplify
    # things, it will simply synced one more time at the end.
    for table in data_r1 data_r2 data_r3 data_r4 data_r1; do
        $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SYSTEM SYNC REPLICA $table CLUSTER"
    done
}

function backup_replicas_thread()
{
    for i in {0..20}; do
        if [[ $i -gt 0 ]]; then
            $CLICKHOUSE_CLIENT -nm -q "
                system drop cluster replica data_r3;
                system drop cluster replica data_r4;
            "
        fi

        $CLICKHOUSE_CLIENT -nm -q "
            drop table if exists data_r3;
            drop table if exists data_r4;

            create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
            create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
        "
        sleep 0.$((RANDOM % 10))
    done
}

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_r1;
    drop table if exists data_r2;
    drop table if exists data_r3;
    drop table if exists data_r4;

    create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
    insert into data_r1 select number key, number%10 part, number value from numbers(100000);
"

backup_replicas_thread

sync_cluster

$CLICKHOUSE_CLIENT -nm -q "
    select _table, count(), length(groupArrayDistinct(_partition_id)) from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
    select count() from data_r1;
    select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
"
