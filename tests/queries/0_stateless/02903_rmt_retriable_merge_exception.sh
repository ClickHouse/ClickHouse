#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that retriable errors during merges/mutations
# (i.e. "No active replica has part X or covering part")
# does not appears as errors (level=Error), only as info message (level=Information).

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists rmt1;
    drop table if exists rmt2;

    create table rmt1 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '1') order by key settings always_fetch_merged_part=1;
    create table rmt2 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '2') order by key settings always_fetch_merged_part=0;

    insert into rmt1 values (1);
    insert into rmt1 values (2);

    system stop pulling replication log rmt2;
    optimize table rmt1 final settings alter_sync=0;
"

# wait while there be at least one 'No active replica has part all_0_1_1 or covering part' in logs
for _ in {0..1000}; do
    no_active_repilica_messages=$($CLICKHOUSE_CLIENT -nm -q "
        system flush logs;
        with
            (select uuid from system.tables where database = currentDatabase() and table = 'rmt1') as uuid_
        select count()
        from system.text_log
        where
            event_date >= yesterday() and event_time >= now() - 600 and
            (
                (logger_name = 'MergeTreeBackgroundExecutor' and message like '%{' || uuid_::String || '::all_0_1_1}%No active replica has part all_0_1_1 or covering part%') or
                (logger_name = uuid_::String || '::all_0_1_1 (MergeFromLogEntryTask)' and message like '%No active replica has part all_0_1_1 or covering part%')
            );
    ")
    if [[ $no_active_repilica_messages -gt 0 ]]; then
        break
    fi
    # too frequent "system flush logs" causes troubles
    sleep 1
done

$CLICKHOUSE_CLIENT -nm -q "
    system start pulling replication log rmt2;
    system flush logs;
    with
        (select uuid from system.tables where database = currentDatabase() and table = 'rmt1') as uuid_
    select
        level, count() > 0
    from system.text_log
    where
        event_date >= yesterday() and event_time >= now() - 600 and
        (
            (logger_name = 'MergeTreeBackgroundExecutor' and message like '%{' || uuid_::String || '::all_0_1_1}%No active replica has part all_0_1_1 or covering part%') or
            (logger_name = uuid_::String || '::all_0_1_1 (MergeFromLogEntryTask)' and message like '%No active replica has part all_0_1_1 or covering part%')
        )
    group by level;
"
