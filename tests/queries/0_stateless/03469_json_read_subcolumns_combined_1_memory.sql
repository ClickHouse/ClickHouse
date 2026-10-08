-- Tags: no-fasttest, long
SET enable_json_type = 1;
set allow_experimental_variant_type = 1;
set use_variant_as_common_type = 1;
set session_timezone = 'UTC';

drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=2, a.b.c UInt32)) engine=Memory;

insert into test select number, '{}' from numbers(5);
insert into test select number, toJSONString(map('a.b.c', number)) from numbers(5, 5);
insert into test select number, toJSONString(map('a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(10, 5);
insert into test select number, toJSONString(map('b.b.d', number::UInt32, 'b.b.e', 'str_' || toString(number))) from numbers(15, 5);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number))) from numbers(20, 5);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', number::UInt32, 'a.b.e', 'str_' || toString(number), 'b.b._' || toString(number), number::UInt32)) from numbers(25, 5);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', range(number % 5 + 1)::Array(UInt32), 'a.b.e', 'str_' || toString(number), 'd.a', number::UInt32, 'd.c', toDate(number))) from numbers(30, 5);
insert into test select number, toJSONString(map('a.b.c', number, 'a.b.d', toDateTime(number), 'a.b.e', 'str_' || toString(number), 'd.a', range(number % 5 + 1)::Array(UInt32), 'd.b', number::UInt32)) from numbers(35, 5);

select distinct arrayJoin(JSONAllPathsWithTypes(json)) as paths_with_types from test order by paths_with_types;

-- Kitchen sink: combined (@), sub-object (^), literal paths and typed subcolumns all together
select json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test order by id format JSONColumns;
select json, json.@non.existing.path, json.@a, json.@a.b, json.@a.b.c, json.@a.b.d, json.@a.b.e, json.@b, json.@b.b.e, json.@d, json.@d.a, json.@d.b, json.^n, json.^a, json.^a.b, json.^b, json.^d, json.a.b.c, json.a.b.d, json.a.b.d.:Int64, json.a.b.e.:String, json.d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.b.:Int64 from test order by id format JSONColumns;

-- Non-existing combined path
select json.@non.existing.path from test order by id format JSONColumns;

-- @a.b.c: typed path, returns Dynamic(UInt32)
select json.@a.b.c, json.a.b.c from test order by id format JSONColumns;

-- @b.b.e: dynamic path (String or Date)
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date from test order by id format JSONColumns;

-- @a.b.d: dynamic path (UInt32, Array(UInt32) or DateTime)
select json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format JSONColumns;

-- @b.b.e and @a.b.d together: tests substreams cache interaction
select json.@b.b.e, json.b.b.e.:String, json.b.b.e.:Date, json.@a.b.d, json.a.b.d.:Int64, json.a.b.d.:Date from test order by id format JSONColumns;

-- @d.a and @d.b: array and scalar dynamic paths
select json.@d.a, json.d.a.:`Array(Nullable(Int64))`, json.d.a.:Date, json.@d.b, json.d.b.:Int64, json.d.b.:Date from test order by id format JSONColumns;

-- @a vs ^a: combined returns sub-object cast to Dynamic when no scalar at path
select json.@a, json.^a from test order by id format JSONColumns;
select json.@a, json.a.b.c from test order by id format JSONColumns;

-- @a.b: intermediate path combined
select json.@a.b, json.^a.b, json.@a.b.c, json.@a.b.d from test order by id format JSONColumns;

-- @b: top-level combined for b-prefix paths
select json.@b, json.^b, json.b.b.e from test order by id format JSONColumns;

-- @d: top-level combined for d-prefix paths
select json.@d, json.^d, json.d.a, json.d.b from test order by id format JSONColumns;

drop table test;
