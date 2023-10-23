-- Test that retriable errors during merges/mutations
-- (i.e. "No active replica has part X or covering part")
-- does not appears as errors (level=Error), only as info message (level=Information).

drop table if exists rmt1;
drop table if exists rmt2;

create table rmt1 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '1') order by key settings always_fetch_merged_part=1;
create table rmt2 (key Int) engine=ReplicatedMergeTree('/clickhouse/{database}', '2') order by key settings always_fetch_merged_part=0;

insert into rmt1 values (1);
insert into rmt1 values (2);

system stop pulling replication log rmt2;
optimize table rmt1 final settings alter_sync=0;

select sleep(3) format Null;
system start pulling replication log rmt2;

system flush logs;
with
    (select uuid from system.tables where database = currentDatabase() and table = 'rmt1') as uuid_
select
    level, count() > 0
from system.text_log
where
    event_date >= yesterday() and event_time >= now() - 60 and
    (
        (logger_name = 'MergeTreeBackgroundExecutor' and message like '%{' || uuid_::String || '::all_0_1_1}%No active replica has part all_0_1_1 or covering part%') or
        (logger_name = uuid_::String || '::all_0_1_1 (MergeFromLogEntryTask)' and message like '%No active replica has part all_0_1_1 or covering part%')
    )
group by level;
