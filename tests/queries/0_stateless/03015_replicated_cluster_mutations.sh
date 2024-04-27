#!/usr/bin/env bash

CLICKHOUSE_CLIENT_OPT+="--allow_experimental_analyzer=0" # FIXME: analyzer is not supported yet


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

$CLICKHOUSE_CLIENT -nm -q "
    -- system stop pulling replication log data_r1;
    -- system stop pulling replication log data_r2;
    -- system stop fetches data_r1;
    -- system stop fetches data_r2;
    system stop replicated sends data_r1;
    system stop replicated sends data_r2;
    insert into data_r1 select number key, number%10 part, number value from numbers(100000);
    select count() from merge(currentDatabase(), '^data_') settings cluster_query_shards=0;
"

$CLICKHOUSE_CLIENT -nm -q "alter table data_r1 update value = -value where 1 settings mutations_sync=0"

echo "Waiting for some rows to be mutated"
i=0 tries=1000
while [[ $i -lt $tries ]]; do
    mutated_rows="$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d@- <<<"select count() from merge(currentDatabase(), '^data_') where value < 0 settings cluster_query_shards=0")"
    if [[ $mutated_rows -gt 0 ]]; then
        echo "Some rows mutated"
        tries=0
        break
    fi
    sleep 0.1
    (( ++i ))
done
if [[ $tries -ne 0 ]]; then
    echo "Rows had not been mutated" >&2
fi

# Adding new replicas with mutations in progress, so that even if those
# replicas will fetch non mutated parts they should fetch the queue entries to
# mutate this parts as well.
$CLICKHOUSE_CLIENT -nm -q "
    create table data_r3 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '3') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    create table data_r4 (key Int, part Int, value Int) engine=ReplicatedMergeTree('/tables/{database}/data', '4') order by key partition by part settings cluster=1, cluster_replication_factor=2;
    select throwIf(is_readonly = 1) from system.replicas where database = currentDatabase() and table like 'data_%' format Null;
"

$CLICKHOUSE_CLIENT -nm -q "
    system start replicated sends data_r1;
    system start replicated sends data_r2;
"

# Retry on "Cannot migrate partition" error
while :; do
    out=$($CLICKHOUSE_CLIENT -nm -q "
        system sync replica data_r1 cluster;
        system sync replica data_r2 cluster;
        system sync replica data_r3 cluster;
        system sync replica data_r4 cluster;
    " 2>&1)
    if [[ $out =~ "Cannot migrate partition" ]]; then
        continue
    fi
    echo -n "$out"
    break
done

$CLICKHOUSE_CLIENT -nm -q "
    system sync replica data_r1 cluster;
    system sync replica data_r2 cluster;
    system sync replica data_r3 cluster;
    system sync replica data_r4 cluster;
"

for table in data_r1 data_r2 data_r3 data_r4; do
    wait_for_all_mutations $table
done

# FIXME:
# - last sync for data_r1 to reflect changes in system.cluster_partitions
# - last sync for everything else to apply all changes after previous sync...
# And all of this should be gone!
$CLICKHOUSE_CLIENT -nm -q "
    system sync replica data_r1 cluster;
    system sync replica data_r2 cluster;
    system sync replica data_r3 cluster;
    system sync replica data_r4 cluster;
"

# NOTE: it is possible to have non mutated parts on old replicas, but only
# when cluster partitions map is not used/not in sync (cluster_query_shards=0),
# consider the following situation:
# - data_r1 has partition p1
# - data_r2 fetched p1 from the data_r1
# - p1 has been migrated to data_r3
# - data_r1 mutated p1 to p1_1
# - data_r2 will not mutate this part, because data_r2 is not in active_replicas for p1 anymore
# And after this you will have non mutated part on data_r2 until it got removed from it.
$CLICKHOUSE_CLIENT -nm -q "
-- { echo }
select _table, count(), countIf(value <= 0), length(groupArrayDistinct(_partition_id)) from merge(currentDatabase(), '^data_') group by _table order by 1 settings cluster_query_shards=0;
select count(), countIf(value <= 0) from data_r1;
select replica, if(length(groupArray(partition)) as cnt == 5, cnt, (cnt - 1)::UInt64) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
select partition, if(count() as cnt == 2, cnt, (cnt - 1)::UInt64) from system.cluster_partitions array join active_replicas as replica where database = currentDatabase() and table = 'data_r1' group by 1 order by 1;
"
