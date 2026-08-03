-- Tags: no-fasttest

set allow_experimental_dynamic_type = 1;

drop table if exists test;
create table test (x UInt64, d Dynamic) engine=Memory;
insert into test select number, number from numbers(4);

select d + 1 as res, toTypeName(res) from test;
select 1 + d as res, toTypeName(res) from test;
select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + d as res, toTypeName(res) from test;
select d + 1 + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;
select d + NULL as res, toTypeName(res) from test;

select d < 2 as res, toTypeName(res) from test;
select d < d as res, toTypeName(res) from test;
select d < x as res, toTypeName(res) from test;
select d > 2 as res, toTypeName(res) from test;
select d > d as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = 2 as res, toTypeName(res) from test;
select d = d as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;
select d = NULL as res, toTypeName(res) from test;

select * from test where d < 2;
select * from test where d > 2;
select * from test where d = 2;
select * from test where d < d;
select * from test where d > d;
select * from test where d = d;
select * from test where d < x;
select * from test where d > x;
select * from test where d = x;
select * from test where d = NULL;

select exp2(d) as res, toTypeName(res) from test;
select sin(d) as res, toTypeName(res) from test;
select cos(d) as res, toTypeName(res) from test;
select tan(d) as res, toTypeName(res) from test;
select mortonEncode(d) as res, toTypeName(res) from test;
select hilbertEncode(d) as res, toTypeName(res) from test;
select bitmaskToList(d) as res, toTypeName(res) from test;
select bitPositionsToArray(d) as res, toTypeName(res) from test;
select isFinite(d) as res, toTypeName(res) from test;
select sipHash64(d) as res, toTypeName(res) from test;
select sipHash128(d) as res, toTypeName(res) from test;
select intHash32(d) as res, toTypeName(res) from test;
select intHash64(d) as res, toTypeName(res) from test;
select h3CellAreaM2(d) as res, toTypeName(res) from test;
select h3CellAreaRads2(d) as res, toTypeName(res) from test;
select h3Distance(d, d) as res, toTypeName(res) from test;
select sqid(d) as res, toTypeName(res) from test;

select sipHash64(d, x) as res, toTypeName(res) from test;

select 'str_' || d as res, toTypeName(res) from test;
select 'str_' || d || x as res, toTypeName(res) from test;
select 'str_' || d || d as res, toTypeName(res) from test;
select 'str_' || d || x || d as res, toTypeName(res) from test;
select d || NULL as res, toTypeName(res) from test;
select 'str_' || d || NULL as res, toTypeName(res) from test;


drop table test;
create table test (x Nullable(UInt64), d Dynamic) engine=Memory;
insert into test select number % 2 ? NULL : number, number from numbers(4);

select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

select 'str_' || d || x as res, toTypeName(res) from test;
select 'str_' || d || x || d as res, toTypeName(res) from test;

drop table test;
create table test (x String, d Dynamic) engine=Memory;
insert into test select 'str_' || number, 'str_' || number from numbers(4);

select d < 'str_2' as res, toTypeName(res) from test;
select d < d as res, toTypeName(res) from test;
select d < x as res, toTypeName(res) from test;
select d > 'str_2' as res, toTypeName(res) from test;
select d > d as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = 'str_2' as res, toTypeName(res) from test;
select d = d as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;
select d = NULL as res, toTypeName(res) from test;

select * from test where d < 'str_2';
select * from test where d > 'str_2';
select * from test where d = 'str_2';
select * from test where d < d;
select * from test where d > d;
select * from test where d = d;
select * from test where d < x;
select * from test where d > x;
select * from test where d = x;
select * from test where d = NULL;

select upper(d) as res, toTypeName(res) from test;
select appendTrailingCharIfAbsent(d, 'd') as res, toTypeName(res) from test;
select match(d, 'str') as res, toTypeName(res) from test;
select concatWithSeparator('|', d, d) as res, toTypeName(res) from test;
select extract(d, '([0-3])') as res, toTypeName(res) from test;
select startsWith(d, 'str') as res, toTypeName(res) from test;
select length(d) as res, toTypeName(res) from test;
select replaceAll(d, 'str', 'a') as res, toTypeName(res) from test;
select repeat(d, 2) as res, toTypeName(res) from test;
select substring(d, 1, 3) as res, toTypeName(res) from test;

truncate table test;
insert into test select 'str_' || number, toFixedString('str_' || number, 5) from numbers(4);

select upper(d) as res, toTypeName(res) from test;
select match(d, 'str') as res, toTypeName(res) from test;
select concatWithSeparator('|', d, d) as res, toTypeName(res) from test;
select startsWith(d, 'str') as res, toTypeName(res) from test;
select length(d) as res, toTypeName(res) from test;
select replaceAll(d, 'str', 'a') as res, toTypeName(res) from test;

drop table test;
create table test (x Nullable(String), d Dynamic) engine=Memory;
insert into test select number % 2 ? NULL : 'str_' || number, 'str_' || number from numbers(4);

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

truncate table test;
insert into test select number % 2 ? NULL : 'str_' || number, toFixedString('str_' || number, 5) from numbers(4);
select upper(d) as res, toTypeName(res) from test;

drop table test;

create table test (x UInt64, d Dynamic) engine=Memory;
insert into test select number, number % 2 ? NULL : number from numbers(4);

select d + 1 as res, toTypeName(res) from test;
select 1 + d as res, toTypeName(res) from test;
select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + d as res, toTypeName(res) from test;
select d + 1 + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;
select d + NULL as res, toTypeName(res) from test;

select d < 2 as res, toTypeName(res) from test;
select d < d as res, toTypeName(res) from test;
select d < x as res, toTypeName(res) from test;
select d > 2 as res, toTypeName(res) from test;
select d > d as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = 2 as res, toTypeName(res) from test;
select d = d as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;
select d = NULL as res, toTypeName(res) from test;

select * from test where d < 2;
select * from test where d > 2;
select * from test where d = 2;
select * from test where d < d;
select * from test where d > d;
select * from test where d = d;
select * from test where d < x;
select * from test where d > x;
select * from test where d = x;
select * from test where d = NULL;

select sipHash64(d) as res, toTypeName(res) from test;
select sipHash64(d, d) as res, toTypeName(res) from test;
select sipHash64(d, x) as res, toTypeName(res) from test;

drop table test;
create table test (x Nullable(UInt64), d Dynamic) engine=Memory;
insert into test select number % 2 ? NULL : number, number % 2 ? NULL : number from numbers(4);

select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

truncate table test;
insert into test select number % 2 ? NULL : number, NULL from numbers(4);

select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

drop table test;
create table test (x String, d Dynamic) engine=Memory;
insert into test select 'str_' || number, number % 2 ? NULL : 'str_' || number from numbers(4);

select d < 'str_2' as res, toTypeName(res) from test;
select d < d as res, toTypeName(res) from test;
select d < x as res, toTypeName(res) from test;
select d > 'str_2' as res, toTypeName(res) from test;
select d > d as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = 'str_2' as res, toTypeName(res) from test;
select d = d as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;
select d = NULL as res, toTypeName(res) from test;

select * from test where d < 'str_2';
select * from test where d > 'str_2';
select * from test where d = 'str_2';
select * from test where d < d;
select * from test where d > d;
select * from test where d = d;
select * from test where d < x;
select * from test where d > x;
select * from test where d = x;
select * from test where d = NULL;

select upper(d) as res, toTypeName(res) from test;

truncate table test;
insert into test select 'str_' || number, number % 2 ? NULL : toFixedString('str_' || number, 5) from numbers(4);
select upper(d) as res, toTypeName(res) from test;

drop table test;

create table test (x Nullable(String), d Dynamic) engine=Memory;
insert into test select number % 2 ? NULL : 'str_' || number, 'str_' || number from numbers(4);

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

truncate table test;

insert into test select number % 2 ? NULL : 'str_' || number, number % 2 ? NULL : toFixedString('str_' || number, 5) from numbers(4);
select upper(d) as res, toTypeName(res) from test;

drop table test;

create table test (x UInt64, d Dynamic(max_types=5)) engine=Memory;
insert into test values (0, NULL), (1, 1::Int8), (2, 2::UInt8), (3, 3::Int16), (4, 4::UInt16), (5, 5::Int32), (6, 6::UInt32), (7, 7::Int64), (8, 8::UInt64), (9, 9::Float32), (10, 10::Float64);

select d + 1 as res, toTypeName(res), dynamicType(res) from test;
select 1 + d as res, toTypeName(res), dynamicType(res) from test;
select d + x as res, toTypeName(res), dynamicType(res) from test;
select x + d as res, toTypeName(res), dynamicType(res) from test;
select d + d as res, toTypeName(res), dynamicType(res) from test;
select d + 1 + d as res, toTypeName(res), dynamicType(res) from test;
select d + x + d as res, toTypeName(res), dynamicType(res) from test;
select d + NULL as res, toTypeName(res), dynamicType(res) from test;

select d < 5 as res, toTypeName(res) from test;
select d < d as res, toTypeName(res) from test;
select d < x as res, toTypeName(res) from test;
select d > 5 as res, toTypeName(res) from test;
select d > d as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = 5 as res, toTypeName(res) from test;
select d = d as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;
select d = NULL as res, toTypeName(res) from test;

select * from test where d < 5;
select * from test where d > 5;
select * from test where d = 5;
select * from test where d < d;
select * from test where d > d;
select * from test where d = d;
select * from test where d < x;
select * from test where d > x;
select * from test where d = x;
select * from test where d = NULL;

select sipHash64(d) as res, toTypeName(res) from test;
select sipHash64(d, d) as res, toTypeName(res) from test;
select sipHash64(d, x) as res, toTypeName(res) from test;

drop table test;
create table test (x Nullable(UInt64), d Dynamic) engine=Memory;
insert into test values (0, NULL), (NULL, 1::Int8), (2, 2::UInt8), (NULL, 3::Int16), (4, 4::UInt16), (NULL, 5::Int32), (6, 6::UInt32), (NULL, 7::Int64), (8, 8::UInt64), (NULL, 9::Float32), (10, 10::Float64);

select d + x as res, toTypeName(res) from test;
select x + d as res, toTypeName(res) from test;
select d + x + d as res, toTypeName(res) from test;

select d < x as res, toTypeName(res) from test;
select d > x as res, toTypeName(res) from test;
select d = x as res, toTypeName(res) from test;

select * from test where d < x;
select * from test where d > x;
select * from test where d = x;

select sipHash64(d, x) as res, toTypeName(res) from test;

drop table test;

create table test (d Dynamic) engine=Memory;
insert into test values ('str_0'), ('str_1'::FixedString(5));

select upper(d) as res, toTypeName(res) from test;

truncate table test;
insert into test select range(number + 1) from numbers(4);
select d[1] as res, toTypeName(res) from test;

truncate table test;
insert into test values ('str'), ([1, 2, 3]), (42::Int32), ('2020-01-01'::Date);

select d + 1 from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select length(d) from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select d[1] from test; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select sipHash64(d) from test;
select sipHash64(d, 42) from test;
select sipHash64(d, d) from test;
