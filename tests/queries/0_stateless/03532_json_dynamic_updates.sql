-- Tags: long

set mutations_sync=1;

drop table if exists test_updates;
create table test_updates (id UInt64, json JSON, dynamic Dynamic) engine=MergeTree order by tuple() settings min_rows_for_wide_part=10000000, min_bytes_for_wide_part=1000000000, index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into test_updates select number, '{"a" : 42, "b" : 42, "c" : 42}', 42::Int64 from numbers(1000000);
alter table test_updates update json = '{"a" : [1, 2, 3], "d" : 42}' where id >= 500000;
alter table test_updates update dynamic = [1, 2, 3]::Array(Int64) where id >= 500000;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) from test_updates order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) from test_updates order by all;
select 'JSON path dynamic types';
select dynamicType(json.a), isDynamicElementInSharedData(json.a), count() from test_updates group by all order by all;
select 'Dynamic types';
select dynamicType(dynamic), isDynamicElementInSharedData(dynamic), count() from test_updates group by all order by all;

select json, json.a, dynamic from test_updates where id in (499999, 500000) order by id;
select json, dynamic from test_updates format Null;

alter table test_updates update json = '{}' where 1;
alter table test_updates update dynamic = NULL where 1;

select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) from test_updates order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) from test_updates order by all;
select 'JSON path dynamic types';
select dynamicType(json.a), isDynamicElementInSharedData(json.a), count() from test_updates group by all order by all;
select 'Dynamic types';
select dynamicType(dynamic), isDynamicElementInSharedData(dynamic), count() from test_updates group by all order by all;

select json, json.a, dynamic from test_updates where id in (499999, 500000) order by id;
select json, dynamic from test_updates format Null;

drop table test_updates;

create table test_updates (id UInt64, json JSON, dynamic Dynamic) engine=MergeTree order by tuple() settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into test_updates select number, '{"a" : 42, "b" : 42, "c" : 42}', 42::Int64 from numbers(1000000);
alter table test_updates update json = '{"a" : [1, 2, 3], "d" : 42}' where id >= 500000;
alter table test_updates update dynamic = [1, 2, 3]::Array(Int64) where id >= 500000;
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) from test_updates order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) from test_updates order by all;
select 'JSON path dynamic types';
select dynamicType(json.a), isDynamicElementInSharedData(json.a), count() from test_updates group by all order by all;
select 'Dynamic types';
select dynamicType(dynamic), isDynamicElementInSharedData(dynamic), count() from test_updates group by all order by all;

select json, json.a, dynamic from test_updates where id in (499999, 500000) order by id;
select json, dynamic from test_updates format Null;

alter table test_updates update json = '{}' where 1;
alter table test_updates update dynamic = NULL where 1;

select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json)) from test_updates order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json)) from test_updates order by all;
select 'JSON path dynamic types';
select dynamicType(json.a), isDynamicElementInSharedData(json.a), count() from test_updates group by all order by all;
select 'Dynamic types';
select dynamicType(dynamic), isDynamicElementInSharedData(dynamic), count() from test_updates group by all order by all;

select json, json.a, dynamic from test_updates where id in (499999, 500000) order by id;
select json, dynamic from test_updates format Null;

drop table test_updates;
