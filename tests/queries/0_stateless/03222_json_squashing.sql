-- Tags: long

set allow_experimental_json_type = 1;
set max_block_size = 1000;

drop table if exists test;

create table test (json JSON) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, '{}'::JSON, number < 3000, '{"a" : 42, "b" : "Hello"}'::JSON, '{"c" : [1, 2, 3], "d" : "2020-01-01"}'::JSON) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(json)) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) as path from test order by path;

truncate table test;
insert into test select multiIf(number < 1000, '{"a" : 42, "b" : "Hello"}'::JSON, number < 3000, '{"c" : [1, 2, 3], "d" : "2020-01-01"}'::JSON, '{"e" : 43, "f" : ["s1", "s2", "s3"]}'::JSON) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(json)) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) as path from test order by path;

drop table test;
create table test (json JSON(max_dynamic_paths=2)) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, '{}'::JSON(max_dynamic_paths=2), number < 3000, '{"a" : 42, "b" : "Hello"}'::JSON(max_dynamic_paths=2), '{"c" : [1, 2, 3], "d" : "2020-01-01"}'::JSON(max_dynamic_paths=2)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(json)) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) as path from test order by path;

truncate table test;
insert into test select multiIf(number < 1000, '{"a" : 42, "b" : "Hello"}'::JSON(max_dynamic_paths=2), number < 3000, '{"c" : [1, 2, 3], "d" : "2020-01-01"}'::JSON(max_dynamic_paths=2), '{"e" : 43, "f" : ["s1", "s2", "s3"]}'::JSON(max_dynamic_paths=2)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(json)) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) as path from test order by path;

truncate table test;
insert into test select multiIf(number < 1000, '{"a" : 42}'::JSON(max_dynamic_paths=2), number < 3000, '{"b" : "Hello", "c" : [1, 2, 3], "d" : "2020-01-01"}'::JSON(max_dynamic_paths=2), '{"e" : 43}'::JSON(max_dynamic_paths=2)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(json)) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) as path from test order by path;

drop table test;
create table test (json JSON(max_dynamic_paths=8)) engine=MergeTree order by tuple();
insert into test select multiIf(number < 1000, '{}'::JSON(max_dynamic_paths=8), number < 3000, materialize('{"a" : [{"b" : 42, "c" : [1, 2, 3]}]}')::JSON(max_dynamic_paths=8), materialize('{"a" : [{"d" : "2020-01-01", "e" : "Hello"}]}')::JSON(max_dynamic_paths=8)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(arrayJoin(json.a[]))) as path from test order by path;

truncate table test;
insert into test select multiIf(number < 1000,  materialize('{"a" : [{"b" : 42, "c" : [1, 2, 3]}]}')::JSON(max_dynamic_paths=8), number < 3000, materialize('{"a" : [{"d" : "2020-01-01", "e" : "Hello"}]}')::JSON(max_dynamic_paths=8), materialize('{"a" : [{"f" : "2020-01-01 00:00:00", "g" : "Hello2"}]}')::JSON(max_dynamic_paths=8)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(arrayJoin(json.a[]))) as path from test order by path;

truncate table test;
insert into test select multiIf(number < 1000,  materialize('{"a" : [{"b" : 42}]}')::JSON(max_dynamic_paths=8), number < 3000, materialize('{"a" : [{"d" : "2020-01-01", "e" : "Hello"}]}')::JSON(max_dynamic_paths=8), materialize('{"a" : [{"f" : "2020-01-01 00:00:00"}]}')::JSON(max_dynamic_paths=8)) from numbers(20000);
select 'All paths';
select distinct arrayJoin(JSONAllPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(arrayJoin(json.a[]))) as path from test order by path;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(arrayJoin(json.a[]))) as path from test order by path;

drop table test;