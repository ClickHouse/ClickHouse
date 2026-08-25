-- Tags: no-ordinary-database

create table data_02491 (key Int) engine=MergeTree() order by tuple() settings old_parts_lifetime=600;
insert into data_02491 values (1);
optimize table data_02491 final;
truncate table data_02491;

system flush logs part_log;
with (select uuid from system.tables where database = currentDatabase() and table = 'data_02491') as table_uuid_
select
    table_uuid != toUUIDOrDefault(Null),
    event_type,
    merge_reason,
    part_name
from system.part_log
where
    database = currentDatabase() and
    table = 'data_02491' and
    table_uuid = table_uuid_
order by event_time_microseconds;

drop table data_02491;
