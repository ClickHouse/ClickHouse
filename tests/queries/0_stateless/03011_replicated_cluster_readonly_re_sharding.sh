#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function sync_cluster()
{
    local table
    for table in data_r1 data_r2 data_r3 data_r4; do
        $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SYSTEM SYNC REPLICA $table CLUSTER"
    done
}

function restart_replica()
{
    local table
    for table in data_r1 data_r2 data_r3 data_r4; do
        $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}" -d "SYSTEM RESTART REPLICA $table"
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
    insert into data_r1 select number key, number%500 part, number value from numbers(100000) settings max_partitions_per_insert_block=1000;

    create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
"

for i in {1..20}; do
    restart_replica &
    # Ignore all errors (that is caused by RESTART REPLICA)
    sync_cluster >& /dev/null
    wait
done

sync_cluster
# the first sync_cluster syncs the cluster to the correct state
# the second sync_cluster syncs cluster partitions map for up-to-date data in system.cluster_partitions
sync_cluster

$CLICKHOUSE_CLIENT -nm -q "
    -- source replica may have more local parts up to 500 (original number of parts),
    -- because parts removal is done in background,
    -- and if the test is fast enough it should have 500,
    -- however if the parts removal had been triggered it may have less local parts, hence min2()
    --
    -- destination replicas should have 250 parts, but sometimes it may have more,
    -- since in case of migration reverts the part had been already placed on a replica,
    -- but this information had not been reflected for the cluster partitions information
    -- (due to some error during migration), so we cannot use strict check as well.
    --
    -- But note, that this is just local parts,
    -- not cluster partitions (system.cluster_partitions) which is used as a source of truth
    -- (and during SELECT cluster partitions will be checked, not local partitions on replicas).
    select _table, min2(count(), 50000), min2(length(groupArrayDistinct(_partition_id)), 250) size from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
    select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
"
