#!/usr/bin/env bash
# Tags: no-fasttest

CLICKHOUSE_CLIENT_OPT+="--allow_experimental_analyzer=0" # FIXME: analyzer is not supported yet

# Tags: stress, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists data_r1;
    drop table if exists data_r2;
    drop table if exists data_r3;
    drop table if exists data_r4;

    create table data_r1 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '1') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    create table data_r2 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '2') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
"

# creates 10 partitions
$CLICKHOUSE_CLIENT -nm -q "
    insert into data_r1 select number key, number%10 part, number value from numbers(100000);
"

mutations_iterations=${mutations_iterations:-100}
replicas_re_create_iterations=${replicas_re_create_iterations:-20}

function mutations_thread()
{
    for i in $(seq 1 $mutations_iterations); do
        $CLICKHOUSE_CLIENT -nm -q "alter table data_r1 update value = value+1 where 1 settings mutations_sync=0"
    done
}

function backup_replicas_thread()
{
    for i in $(seq 1 $replicas_re_create_iterations); do
        if [[ $i -gt 1 ]]; then
            # Ignore error "Cannot process partition ... will restart"
            $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal -nm -q "
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

mutations_thread &
backup_replicas_thread &
wait

$CLICKHOUSE_CLIENT -nm -q "
    system sync replica data_r1;
    system sync replica data_r2;
    system sync replica data_r3;
    system sync replica data_r4;
"

for table in data_r1 data_r2 data_r3 data_r4; do
    wait_for_all_mutations $table
done

# after previous we may have newly added entries to process (i.e. mutations),
# so regular replication queue should be synced as well
$CLICKHOUSE_CLIENT -nm -q "
    system sync replica data_r1;
    system sync replica data_r2;
    system sync replica data_r3;
    system sync replica data_r4;
"

# last sync to reflect changes in system.cluster_partitions
$CLICKHOUSE_CLIENT -q "system sync replica data_r1 cluster"

$CLICKHOUSE_CLIENT -nm -q "
-- { echo }
select _table, count() = countIf(value = key+$mutations_iterations) from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
select count(), countIf(value = key+$mutations_iterations), length(groupArray(_partition_id)) from data_r1;
select replica, length(groupArray(partition)) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select partition, count() from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
"

# no overlapped mutations!
$CLICKHOUSE_CLIENT -nm -q "
WITH splitByChar('/', path) AS parts
SELECT
    parts[3] AS database,
    extract(data, 'mutate\n([^\\s]*)\n') AS from_part,
    groupUniqArray(path) AS paths,
    groupUniqArray(data) AS mutations,
    groupUniqArray(extract(data, 'to\n([^\\s]*)\n')) AS to_parts
FROM system.zookeeper_log
WHERE (type = 'Response') AND (op_num = 'Create') AND (data LIKE '%mutate%') AND (error = 'ZOK') AND (path LIKE '%/log/%') AND (startsWith(path, '/tables/' || currentDatabase() || '/%'))
GROUP BY
    1,
    2
HAVING length(mutations) > 1
LIMIT 10
"
