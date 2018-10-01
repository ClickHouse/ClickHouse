set allow_experimental_low_cardinality_type = 1;

drop table if exists test.lc_str_0;
drop table if exists test.lc_str_1;
drop table if exists test.lc_null_str_0;
drop table if exists test.lc_null_str_1;
drop table if exists test.lc_int8_0;
drop table if exists test.lc_int8_1;
drop table if exists test.lc_null_int8_0;
drop table if exists test.lc_null_int8_1;
drop table if exists test.lc_fix_str_0;
drop table if exists test.lc_fix_str_1;
drop table if exists test.lc_null_fix_str_0;
drop table if exists test.lc_null_fix_str_1;

create table test.lc_str_0 (str LowCardinality(String)) engine = Memory;
create table test.lc_str_1 (str StringWithDictionary) engine = Memory;
create table test.lc_null_str_0 (str LowCardinality(Nullable(String))) engine = Memory;
create table test.lc_null_str_1 (str NullableWithDictionary(String)) engine = Memory;
create table test.lc_int8_0 (val LowCardinality(Int8)) engine = Memory;
create table test.lc_int8_1 (val Int8WithDictionary) engine = Memory;
create table test.lc_null_int8_0 (val LowCardinality(Nullable(Int8))) engine = Memory;
create table test.lc_null_int8_1 (val NullableWithDictionary(Int8)) engine = Memory;
create table test.lc_fix_str_0 (str LowCardinality(FixedString(2))) engine = Memory;
create table test.lc_fix_str_1 (str FixedStringWithDictionary(2)) engine = Memory;
create table test.lc_null_fix_str_0 (str LowCardinality(Nullable(FixedString(2)))) engine = Memory;
create table test.lc_null_fix_str_1 (str NullableWithDictionary(FixedString(2))) engine = Memory;

insert into test.lc_str_0 select 'a';
insert into test.lc_str_1 select 'a';
insert into test.lc_null_str_0 select 'a';
insert into test.lc_null_str_1 select 'a';
insert into test.lc_int8_0 select 1;
insert into test.lc_int8_1 select 1;
insert into test.lc_null_int8_0 select 1;
insert into test.lc_null_int8_1 select 1;
insert into test.lc_fix_str_0 select 'ab';
insert into test.lc_fix_str_1 select 'ab';
insert into test.lc_null_fix_str_0 select 'ab';
insert into test.lc_null_fix_str_1 select 'ab';

select str from test.lc_str_0;
select str from test.lc_str_1;
select str from test.lc_null_str_0;
select str from test.lc_null_str_1;
select val from test.lc_int8_0;
select val from test.lc_int8_1;
select val from test.lc_null_int8_0;
select val from test.lc_null_int8_1;
select str from test.lc_fix_str_0;
select str from test.lc_fix_str_1;
select str from test.lc_null_fix_str_0;
select str from test.lc_null_fix_str_1;

drop table if exists test.lc_str_0;
drop table if exists test.lc_str_1;
drop table if exists test.lc_null_str_0;
drop table if exists test.lc_null_str_1;
drop table if exists test.lc_int8_0;
drop table if exists test.lc_int8_1;
drop table if exists test.lc_null_int8_0;
drop table if exists test.lc_null_int8_1;
drop table if exists test.lc_fix_str_0;
drop table if exists test.lc_fix_str_1;
drop table if exists test.lc_null_fix_str_0;
drop table if exists test.lc_null_fix_str_1;

select '-';
SELECT toLowCardinality('a') AS s, toTypeName(s), toTypeName(length(s)) from system.one;
select toLowCardinality('a') as val group by val;
select (toLowCardinality('a') as val) || 'b' group by val;
select toLowCardinality(z) as val from (select arrayJoin(['c', 'd']) as z) group by val;
select (toLowCardinality(z) as val) || 'b'  from (select arrayJoin(['c', 'd']) as z) group by val;

