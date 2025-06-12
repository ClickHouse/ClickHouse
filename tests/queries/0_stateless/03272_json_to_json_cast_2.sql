-- Tags: long

SET enable_json_type = 1;
set enable_analyzer = 1;
set output_format_native_write_json_as_string = 0;

drop table if exists test;
create table test (json JSON(max_dynamic_paths=4)) engine=Memory;
insert into test format JSONAsObject
{"k1" : 1}
{"k1" : 2, "k4" : 22}
{"k1" : 3, "k4" : 23, "k3" : 33}
{"k1" : 4, "k4" : 24, "k3" : 34, "k2" : 44};

select 'max_dynamic_paths=3';
select json::JSON(max_dynamic_paths=3) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4 from test;
select 'max_dynamic_paths=2';
select json::JSON(max_dynamic_paths=2) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4 from test;
select 'max_dynamic_paths=1';
select json::JSON(max_dynamic_paths=1) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4 from test;
select 'max_dynamic_paths=0';
select json::JSON(max_dynamic_paths=0) as json2, JSONDynamicPaths(json2), JSONSharedDataPaths(json2), json2.k1, json2.k2, json2.k3, json2.k4 from test;

drop table test;

set max_block_size=10000;
set max_threads=1;
create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1;
insert into test select number, multiIf(number < 10000, '{"k2" : 42}', number < 30000, '{"k3" : 42}', number < 60000, '{"k4" : 42}', number < 100000, '{"k1" : 42}', '{"k1" : 42, "k2" : 42, "k3" : 42, "k4" : 42}') from numbers(150000);

select 'max_dynamic_paths=3';
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json2)) from (select json::JSON(max_dynamic_paths=3) as json2 from test) order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json2)) from (select json::JSON(max_dynamic_paths=3) as json2 from test) order by all;
select 'max_dynamic_paths=2';
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json2)) from (select json::JSON(max_dynamic_paths=2) as json2 from test) order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json2)) from (select json::JSON(max_dynamic_paths=2) as json2 from test) order by all;
select 'max_dynamic_paths=1';
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json2)) from (select json::JSON(max_dynamic_paths=1) as json2 from test) order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json2)) from (select json::JSON(max_dynamic_paths=1) as json2 from test) order by all;
select 'max_dynamic_paths=0';
select 'Dynamic paths';
select distinct arrayJoin(JSONDynamicPaths(json2)) from (select json::JSON(max_dynamic_paths=0) as json2 from test) order by all;
select 'Shared data paths';
select distinct arrayJoin(JSONSharedDataPaths(json2)) from (select json::JSON(max_dynamic_paths=0) as json2 from test) order by all;

drop table test;
