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

    create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by (part, key % 10) settings cluster=1, cluster_replication_factor=2;
    create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by (part, key % 10) settings cluster=1, cluster_replication_factor=2;
    create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by (part, key % 10) settings cluster=1, cluster_replication_factor=2;
    create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by (part, key % 10) settings cluster=1, cluster_replication_factor=2;

    insert into data_r1 select number key, number%2 part, number value from numbers(100);

    system sync replica data_r1 cluster;
    system sync replica data_r2 cluster;
    system sync replica data_r3 cluster;
    system sync replica data_r4 cluster;
"

# FIXME(cluster): ALTER TABLE DROP PART cannot be executed from any replicas,
# but only from the replica that contains this part, since right now replicas
# in cluster mode stores only parts that are related to them.
#
# This can be fixed, but I decided to keep it as-is for now, to avoid interface
# changes (since this will likely increase the conflicts probability as well as
# complexity)
replica_name=$($CLICKHOUSE_CLIENT -q "select active_replicas[1] from system.cluster_partitions where database = currentDatabase() and table like 'data_%' and partition = '0-0' limit 1")
replica_table=$($CLICKHOUSE_CLIENT -q "select table from system.replicas where database = currentDatabase() and replica_name = '$replica_name'")

$CLICKHOUSE_CLIENT -q "alter table $replica_table drop part '0-0_0_0_0' settings mutations_sync=2"

$CLICKHOUSE_CLIENT -nm -q "
    select count() from data_r1;
    select count() from data_r2;
    select count() from data_r3;
    select count() from data_r4;
"
