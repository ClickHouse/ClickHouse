-- Tags: no-fasttest, long
-- Random settings limits: index_granularity=(100, None); index_granularity_bytes=(100000, None); max_threads=(4, 32)

SET enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set session_timezone = 'UTC';

drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=2, a.b.c UInt32)) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;

insert into test select number, '{}' from numbers(10000);
insert into test select number, toJSONString(map('a.b.c', number)) from numbers(10000, 10000);
insert into test select number, toJSONString(map('a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(20000, 10000);
insert into test select number, toJSONString(map('b.b.d', number::UInt32, 'b.b.e', 'str_' || toString(number))) from numbers(30000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(40000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number), 'b.b._' || toString(number % 5), number::UInt32)) from numbers(50000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', range(number % 5 + 1)::Array(UInt32), 'a.b.e', 'str_' || toString(number), 'd.a', number::UInt32, 'd.c', toDate(number))) from numbers(60000, 10000);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', toDateTime(number), 'a.b.e', 'str_' || toString(number), 'd.a', range(number % 5 + 1)::Array(UInt32), 'd.b', number::UInt32)) from numbers(70000, 10000);

select distinct arrayJoin(JSONAllPathsWithTypes(json)) as paths_with_types from test order by paths_with_types;

select count() from test where json.@non.existing.path is Null;
select json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test format Null;
select json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test order by id format Null;
select json, json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test format Null;
select json, json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test order by id format Null;

select count() from test where json.@a.b.c is Null;
select json.@a.b.c from test format Null;
select json.@a.b.c from test order by id format Null;
select json.@a.b.c, json.a.b.c from test format Null;
select json.@a.b.c, json.a.b.c from test order by id format Null;
select json, json.@a.b.c from test format Null;
select json, json.@a.b.c from test order by id format Null;

select count() from test where json.@b.b.e is Null;
select json.@b.b.e from test format Null;
select json.@b.b.e from test order by id format Null;
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date from test format Null;
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date from test order by id format Null;
select json, json.@b.b.e from test format Null;
select json, json.@b.b.e from test order by id format Null;
select json, json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date from test format Null;
select json, json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date from test order by id format Null;

select count() from test where json.@a.b.d is Null;
select json.@a.b.d from test format Null;
select json.@a.b.d from test order by id format Null;
select json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test format Null;
select json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format Null;
select json, json.@a.b.d from test format Null;
select json, json.@a.b.d from test order by id format Null;
select json, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test format Null;
select json, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format Null;

select count() from test where json.@b.b.e is Null and json.@a.b.d is Null;
select json.@b.b.e, json.@a.b.d from test order by id format Null;
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test format Null;
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format Null;
select json, json.@b.b.e, json.@a.b.d from test format Null;
select json, json.@b.b.e, json.@a.b.d from test order by id format Null;
select json, json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test format Null;
select json, json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format Null;

select count() from test where json.@d.a is Null and json.@d.b is Null;
select json.@d.a, json.@d.b from test order by id format Null;
select json.@d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.a.:Date, json.@d.b, json.d.b.:Int64, json.d.b.:Date from test format Null;
select json.@d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.a.:Date, json.@d.b, json.d.b.:Int64, json.d.b.:Date from test order by id format Null;
select json, json.@d.a, json.@d.b from test format Null;
select json, json.@d.a, json.@d.b from test order by id format Null;
select json, json.@d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.a.:Date, json.@d.b, json.d.b.:Int64, json.d.b.:Date from test format Null;
select json, json.@d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.a.:Date, json.@d.b, json.d.b.:Int64, json.d.b.:Date from test order by id format Null;

select count() from test where empty(json.@a) and json.a.b.c == 0;
select json.@a, json.^a from test order by id format Null;
select json.@a, json.a.b.c from test format Null;
select json.@a, json.a.b.c from test order by id format Null;
select json, json.@a, json.^a, json.a.b.c from test format Null;
select json, json.@a, json.^a, json.a.b.c from test order by id format Null;

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
