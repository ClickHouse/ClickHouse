
SET enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;

drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=8, a.b Array(JSON))) engine=MergeTree order by id settings min_rows_for_wide_part=1000000000, min_bytes_for_wide_part=10000000000;

insert into test select number, '{}' from numbers(10000);
insert into test select number, toJSONString(map('a.b', arrayMap(x -> map('b.c.d_' || toString(x), number::UInt32, 'c.d.e', range((number + x) % 5 + 1)), range(number % 5 + 1)))) from numbers(10000, 10000);
insert into test select number, toJSONString(map('a.r', arrayMap(x -> map('b.c.d_' || toString(x), number::UInt32, 'c.d.e', range((number + x) % 5 + 1)), range(number % 5 + 1)))) from numbers(20000, 10000);
insert into test select number, toJSONString(map('a.a1', number, 'a.a2', number, 'a.a3', number, 'a.a4', number, 'a.a5', number, 'a.a6', number, 'a.a7', number, 'a.a8', number, 'a.r', arrayMap(x -> map('b.c.d_' || toString(x), number::UInt32, 'c.d.e', range((number + x) % 5 + 1)), range(number % 5 + 1)))) from numbers(30000, 10000);

select distinct arrayJoin(JSONAllPathsWithTypes(json)) as paths_with_types from test order by paths_with_types;
select distinct arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b))) as paths_with_types from test order by paths_with_types;
select distinct arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.r[]))) as paths_with_types from test order by paths_with_types;

select json, json.a.b, json.a.b.c, json.a.b.c.d.e, json.a.b.b.c.d_0, json.a.b.b.c.d_1, json.a.b.b.c.d_2, json.a.b.b.c.d_3, json.a.b.b.c.d_4, json.a.r, json.a.r[], json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1, json.a.r[].b.c.d_2, json.a.r[].b.c.d_3, json.a.r[].b.c.d_4, json.^a, json.a.b.^b.c, json.a.r[].^b.c from test format Null;
select json, json.a.b, json.a.b.c, json.a.b.c.d.e, json.a.b.b.c.d_0, json.a.b.b.c.d_1, json.a.b.b.c.d_2, json.a.b.b.c.d_3, json.a.b.b.c.d_4, json.a.r, json.a.r[], json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1, json.a.r[].b.c.d_2, json.a.r[].b.c.d_3, json.a.r[].b.c.d_4, json.^a, json.a.b.^b.c, json.a.r[].^b.c from test order by id format Null;
select json.a.b, json.a.b.c, json.a.b.c.d.e, json.a.b.b.c.d_0, json.a.b.b.c.d_1, json.a.b.b.c.d_2, json.a.b.b.c.d_3, json.a.b.b.c.d_4, json.a.r, json.a.r[], json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1, json.a.r[].b.c.d_2, json.a.r[].b.c.d_3, json.a.r[].b.c.d_4, json.^a, json.a.b.^b.c, json.a.r[].^b.c from test format Null;
select json.a.b, json.a.b.c, json.a.b.c.d.e, json.a.b.b.c.d_0, json.a.b.b.c.d_1, json.a.b.b.c.d_2, json.a.b.b.c.d_3, json.a.b.b.c.d_4, json.a.r, json.a.r[], json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1, json.a.r[].b.c.d_2, json.a.r[].b.c.d_3, json.a.r[].b.c.d_4, json.^a, json.a.b.^b.c, json.a.r[].^b.c from test order by id format Null;

select count() from test where empty(json.a.r[].c.d.e) and empty(json.a.r[].b.c.d_0) and empty(json.a.r[].b.c.d_1);
select count() from test where empty(json.a.r[].c.d.e.:`Array(Nullable(Int64))`) and empty(json.a.r[].b.c.d_0.:Int64) and empty(json.a.r[].b.c.d_1.:Int64);
select count() from test where arrayJoin(json.a.r[].c.d.e) is null and arrayJoin(json.a.r[].b.c.d_0) is null and arrayJoin(json.a.r[].b.c.d_1) is null;
select count() from test where arrayJoin(json.a.r[].c.d.e.:`Array(Nullable(Int64))`) is null and arrayJoin(json.a.r[].b.c.d_0.:Int64) is null and arrayJoin(json.a.r[].b.c.d_1.:Int64) is null;

select json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1 from test format Null;
select json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1 from test order by id format Null;
select json.a.r[].c.d.e.:`Array(Nullable(Int64))`, json.a.r[].b.c.d_0.:Int64, json.a.r[].b.c.d_1.:Int64 from test format Null;
select json.a.r[].c.d.e.:`Array(Nullable(Int64))`, json.a.r[].b.c.d_0.:Int64, json.a.r[].b.c.d_1.:Int64 from test order by id format Null;
select json.a.r, json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1 from test format Null;
select json.a.r, json.a.r[].c.d.e, json.a.r[].b.c.d_0, json.a.r[].b.c.d_1 from test order by id format Null;
select json.a.r, json.a.r[].c.d.e.:`Array(Nullable(Int64))`, json.a.r[].b.c.d_0.:Int64, json.a.r[].b.c.d_1.:Int64 from test format Null;
select json.a.r, json.a.r[].c.d.e.:`Array(Nullable(Int64))`, json.a.r[].b.c.d_0.:Int64, json.a.r[].b.c.d_1.:Int64 from test order by id format Null;

select count() from test where empty(json.a.r[].^b) and empty(json.a.r[].^b.c) and empty(json.a.r[].b.c.d_0);
select count() from test where empty(json.a.r[].^b) and empty(json.a.r[].^b.c) and empty(json.a.r[].b.c.d_0.:Int64);
select count() from test where empty(arrayJoin(json.a.r[].^b)) and empty(arrayJoin(json.a.r[].^b.c)) and arrayJoin(json.a.r[].b.c.d_0) is null;
select count() from test where empty(arrayJoin(json.a.r[].^b)) and empty(arrayJoin(json.a.r[].^b.c)) and arrayJoin(json.a.r[].b.c.d_0.:Int64) is null;

select json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0 from test format Null;
select json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0 from test order by id format Null;
select json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0.:Int64 from test format Null;
select json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0.:Int64 from test order by id format Null;
select json.a.r, json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0 from test format Null;
select json.a.r, json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0 from test order by id format Null;
select json.a.r, json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0.:Int64 from test format Null;
select json.a.r, json.a.r[].^b, json.a.r[].^b.c, json.a.r[].b.c.d_0.:Int64 from test order by id format Null;

drop table test;
