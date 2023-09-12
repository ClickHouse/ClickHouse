#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function sync_cluster()
{
    local table
    # errors are fatal
    for table in data_r1 data_r2; do
        $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SYSTEM SYNC REPLICA $table CLUSTER"
    done
    # errors are ignored
    for table in data_r3 data_r4; do
        $CLICKHOUSE_CURL -s "${CLICKHOUSE_URL}" -d "SYSTEM SYNC REPLICA $table CLUSTER"
    done >& /dev/null
}

function backup_replicas_thread()
{
    # NOTE: this actions will lead to data loss, since DROP does not migrate
    # partitions back to alive replicas, and between CREATE and DROP some
    # partitions may be completelly migrated to this new replicas, and after
    # DROP nobody will have them.
    #
    # I guess some option do migrate partitions back to other replicas on DROP
    # should be introduced?
    for i in {0..20}; do
        $CLICKHOUSE_CLIENT -nm -q "
            drop table if exists data_r3;
            drop table if exists data_r4;

            create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
            create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
        " |& grep "DB::Exception: " | grep -v -e NOT_IMPLEMENTED
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

backup_replicas_thread &
wait

sync_cluster

$CLICKHOUSE_CLIENT -nm -q "
    select _table, min2(count(), 50000), min2(length(groupArrayDistinct(_partition_id)), 250) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
    select count() from data_r1;
    -- FIXME: during table flaps it is possible that backup replicas will have
    -- unique partitions, which will lead to data loss eventually, as a workaround
    -- right now cluster_replication_factor=1 is not allowed, but better to have
    -- some command to drop replica with proper migrations
    --
    -- select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
"
