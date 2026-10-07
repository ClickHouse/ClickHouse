-- Tags: no-fasttest, long

SET enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set session_timezone = 'UTC';

set min_bytes_to_use_direct_io = 0; -- min_bytes_to_use_direct_io > 0 is broken
-- Override randomized max_threads to avoid timeout with random settings
SET max_threads=0;

drop table if exists test;
-- Pin settings that cause timeouts with random MergeTree settings while keeping JSON-related ones randomized.
create table test (id UInt64, json JSON(max_dynamic_paths=2, a.b.c UInt32)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity=8192, prewarm_primary_key_cache=0, prewarm_mark_cache=0, merge_max_block_size=8192, auto_statistics_types='';

insert into test select number, '{}' from numbers(10000);
insert into test select number, toJSONString(map('a.b.c', number)) from numbers(10000, 10000);
insert into test select number, toJSONString(map('a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(20000, 10000);
insert into test select number, toJSONString(map('b.b.d', number::UInt32, 'b.b.e', 'str_' || toString(number))) from numbers(30000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(40000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number), 'b.b._' || toString(number % 5), number::UInt32)) from numbers(50000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', range(number % 5 + 1)::Array(UInt32), 'a.b.e', 'str_' || toString(number), 'd.a', number::UInt32, 'd.c', toDate(number))) from numbers(60000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', toDateTime(number), 'a.b.e', 'str_' || toString(number), 'd.a', range(number % 5 + 1)::Array(UInt32), 'd.b', number::UInt32)) from numbers(70000, 10000);

select count() from test where empty(json.@a.b) and json.a.b.c == 0;
select json.@a.b, json.^a.b from test order by id format Null;
select json.@a.b, json.@a.b.c, json.@a.b.d from test format Null;
select json.@a.b, json.@a.b.c, json.@a.b.d from test order by id format Null;
select json, json.@a.b, json.^a.b, json.@a.b.c from test format Null;
select json, json.@a.b, json.^a.b, json.@a.b.c from test order by id format Null;

select count() from test where empty(json.@b);
select json.@b, json.^b from test order by id format Null;
select json.@b, json.b.b.e, json.b.b.`_0`, json.b.b.`_1` from test format Null;
select json.@b, json.b.b.e, json.b.b.`_0`, json.b.b.`_1` from test order by id format Null;
select json, json.@b, json.^b, json.b.b.e from test format Null;
select json, json.@b, json.^b, json.b.b.e from test order by id format Null;

select count() from test where empty(json.@d);
select json.@d, json.^d from test order by id format Null;
select json.@d, json.d.a, json.d.b from test format Null;
select json.@d, json.d.a, json.d.b from test order by id format Null;
select json, json.@d, json.^d, json.d.a, json.d.b from test format Null;
select json, json.@d, json.^d, json.d.a, json.d.b from test order by id format Null;

drop table test;
