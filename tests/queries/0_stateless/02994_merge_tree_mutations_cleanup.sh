#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ReplicatedMergeTree requires ZooKeeper

# With `finished_mutations_to_keep = 0` no entries of finished mutations are kept:
# the cleanup thread removes the entry of a mutation once the mutation is finished.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Waits until the table has no mutation entries.
function wait_for_no_mutation_entries()
{
    local table="$1"
    while clickhouse_test_time_left
    do
        if [[ $($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = '$table'") == "0" ]]
        then
            return
        fi
        sleep 0.2
    done
}

# The cleanup thread wakes up every second.
# SharedMergeTree also needs a fresh set of replicas to find out that the mutation is finished on all of them.
CLEANUP_SETTINGS="finished_mutations_to_keep=0, merge_tree_clear_old_parts_interval_seconds=1, cleanup_delay_period=1, max_cleanup_delay_period=1, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0, shared_merge_tree_replica_set_max_lifetime_seconds=1"

$CLICKHOUSE_CLIENT -q "
    drop table if exists data_rmt;
    drop table if exists data_mt;

    create table data_rmt (key Int) engine=ReplicatedMergeTree('/tables/{database}/data', '{table}') order by tuple() settings $CLEANUP_SETTINGS;
    create table data_mt (key Int) engine=MergeTree() order by tuple() settings $CLEANUP_SETTINGS;
"

for table in data_rmt data_mt
do
    # The entry of the mutation may be removed even before the query returns, and the query must not fail because of that.
    $CLICKHOUSE_CLIENT -q "
        insert into $table values (1);
        alter table $table delete where 1 settings mutations_sync = 2;
        select 'rows after the mutation in $table', count() from $table;
    "

    wait_for_no_mutation_entries $table
    $CLICKHOUSE_CLIENT -q "
        select 'mutations after cleanup for $table', count() from system.mutations where database = currentDatabase() and table = '$table';
        drop table $table;
    "
done
