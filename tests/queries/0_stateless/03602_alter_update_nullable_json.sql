set mutations_sync=1;
set max_block_size=1000;

drop table if exists test;
create table test (id UInt32, json Nullable(JSON(max_dynamic_paths=1))) engine=MergeTree order by tuple() settings min_bytes_for_wide_part='200G';
insert into test select number, '{"a" : 1}' from numbers(100000);
alter table test update json='{"b" : 1}' where id > 90000;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(assumeNotNull(json))) from test;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(assumeNotNull(json))) from test;

drop table test;

create table test (id UInt32, json Nullable(JSON(max_dynamic_paths=1))) engine=MergeTree order by tuple() settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1;
insert into test select number, '{"a" : 1}' from numbers(100000);
alter table test update json='{"b" : 1}' where id > 90000;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(assumeNotNull(json))) from test;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(assumeNotNull(json))) from test;

drop table test;


