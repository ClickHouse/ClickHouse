#!/usr/bin/env bash
# Tags: no-ordinary-database
# Tag no-ordinary-database: requires UUID

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that retriable errors during merges/mutations
# (i.e. "No active replica has part X or covering part")
# does not appears as errors (level=Error), only as info message (level=Information).

cluster=test_shard_localhost
if [[ $($CLICKHOUSE_CLIENT -q "select count()>0 from system.clusters where cluster = 'test_cluster_database_replicated'") = 1 ]]; then
    cluster=test_cluster_database_replicated
fi

$CLICKHOUSE_CLIENT -m --distributed_ddl_output_mode=none -q "
    drop table if exists rmt1;
    drop table if exists rmt2;

    create table rmt1 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '1') order by key settings always_fetch_merged_part=1;
    create table rmt2 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '2') order by key settings always_fetch_merged_part=0;

    insert into rmt1 values (1);
    insert into rmt1 values (2);

    system sync replica rmt1;
    -- SYSTEM STOP PULLING REPLICATION LOG does not waits for the current pull,
    -- trigger it explicitly to 'avoid race' (though proper way will be to wait
    -- for current pull in the StorageReplicatedMergeTree::getActionLock())
    system sync replica rmt2;
    -- NOTE: CLICKHOUSE_DATABASE is required
    system stop pulling replication log on cluster $cluster $CLICKHOUSE_DATABASE.rmt2;
    optimize table rmt1 final settings alter_sync=0, optimize_throw_if_noop=1;
" || exit 1

table_uuid=$($CLICKHOUSE_CLIENT -q "select uuid from system.tables where database = currentDatabase() and table = 'rmt1'")
if [[ -z $table_uuid ]]; then
    echo "Table does not have UUID" >&2
    exit 1
fi

# NOTE: that part name can be different from all_0_1_1, in case of ZooKeeper retries
part_name='%'

# wait while there be at least one 'No active replica has part all_0_1_1 or covering part' in logs
for _ in {0..50}; do
    no_active_repilica_messages=$($CLICKHOUSE_CLIENT -m -q "
        system flush logs;

        select count()
        from system.text_log
        where
            event_date >= yesterday() and event_time >= now() - 600 and
            (
                (logger_name = 'MergeTreeBackgroundExecutor' and message like '%{$table_uuid::$part_name}%No active replica has part $part_name or covering part%') or
                (logger_name like '$table_uuid::$part_name (MergeFromLogEntryTask)' and message like '%No active replica has part $part_name or covering part%')
            )
        SETTINGS max_rows_to_read = 0;
    ")
    if [[ $no_active_repilica_messages -gt 0 ]]; then
        break
    fi
    # too frequent "system flush logs" causes troubles
    sleep 1
done

$CLICKHOUSE_CLIENT -m -q "
    system start pulling replication log rmt2;
    system flush logs;

    select
        level, count() > 0
    from system.text_log
    where
        event_date >= yesterday() and event_time >= now() - 600 and
        (
            (logger_name = 'MergeTreeBackgroundExecutor' and message like '%{$table_uuid::$part_name}%No active replica has part $part_name or covering part%') or
            (logger_name like '$table_uuid::$part_name (MergeFromLogEntryTask)' and message like '%No active replica has part $part_name or covering part%')
        )
    group by level
    SETTINGS max_rows_to_read = 0;
"
