#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ReplicatedMergeTree requires ZooKeeper

# With `finished_mutations_to_keep = 0` no entries of finished mutations are kept:
# the cleanup thread removes the entry of a mutation once the mutation is finished.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Waits until the table has the given number of mutation entries.
function wait_for_mutation_entries()
{
    local table="$1"
    local expected="$2"
    while clickhouse_test_time_left
    do
        if [[ $($CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = '$table'") == "$expected" ]]
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
    $CLICKHOUSE_CLIENT -q "
        insert into $table values (1);

        -- The mutation cannot be finished while merges are stopped, and the entry of an unfinished mutation is never removed.
        system stop merges $table;
        alter table $table delete where 1 settings mutations_sync = 0;
    "
    wait_for_mutation_entries $table 1
    $CLICKHOUSE_CLIENT -q "
        select 'unfinished mutations for $table', count() from system.mutations where database = currentDatabase() and table = '$table' and not is_done;
        system start merges $table;
    "

    # Now the mutation gets finished and its entry is removed.
    wait_for_mutation_entries $table 0
    $CLICKHOUSE_CLIENT -q "
        select 'mutations after cleanup for $table', count() from system.mutations where database = currentDatabase() and table = '$table';
        select 'rows in $table', count() from $table;
        drop table $table;
    "
done
