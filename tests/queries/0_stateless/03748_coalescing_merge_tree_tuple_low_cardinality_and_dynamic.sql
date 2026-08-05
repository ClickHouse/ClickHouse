set mutations_sync=1;
drop table if exists test;
create table test (id UInt64, t Tuple(a LowCardinality(String), json JSON)) engine=CoalescingMergeTree order by id settings min_bytes_for_wide_part=1, min_rows_for_wide_part=1, index_granularity=32, merge_max_block_size=32;
insert into test select number, tuple('str', '{}') from numbers(100);
alter table test update t = tuple('str', '{"a" : 42}') where id > 90;
optimize table test final;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(t.json)) from test;
select 'Shared data parhs';
select distinct arrayJoin(JSONSharedDataPaths(t.json)) from test;
drop table test;

