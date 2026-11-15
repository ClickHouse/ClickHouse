set allow_suspicious_low_cardinality_types = 1;
drop table if exists lc_str_0;
drop table if exists lc_str_1;
drop table if exists lc_null_str_0;
drop table if exists lc_null_str_1;
drop table if exists lc_int8_0;
drop table if exists lc_int8_1;
drop table if exists lc_null_int8_0;
drop table if exists lc_null_int8_1;
drop table if exists lc_fix_str_0;
drop table if exists lc_fix_str_1;
drop table if exists lc_null_fix_str_0;
drop table if exists lc_null_fix_str_1;

create table lc_str_0 (str LowCardinality(String)) engine = Memory;
create table lc_null_str_0 (str LowCardinality(Nullable(String))) engine = Memory;
create table lc_int8_0 (val LowCardinality(Int8)) engine = Memory;
create table lc_null_int8_0 (val LowCardinality(Nullable(Int8))) engine = Memory;
create table lc_fix_str_0 (str LowCardinality(FixedString(2))) engine = Memory;
create table lc_null_fix_str_0 (str LowCardinality(Nullable(FixedString(2)))) engine = Memory;

insert into lc_str_0 select 'a';
insert into lc_null_str_0 select 'a';
insert into lc_int8_0 select 1;
insert into lc_null_int8_0 select 1;
insert into lc_fix_str_0 select 'ab';
insert into lc_null_fix_str_0 select 'ab';

select str from lc_str_0;
select str from lc_null_str_0;
select val from lc_int8_0;
select val from lc_null_int8_0;
select str from lc_fix_str_0;
select str from lc_null_fix_str_0;

drop table if exists lc_str_0;
drop table if exists lc_null_str_0;
drop table if exists lc_int8_0;
drop table if exists lc_null_int8_0;
drop table if exists lc_fix_str_0;
drop table if exists lc_null_fix_str_0;

select '-';
SELECT toLowCardinality('a') AS s, toTypeName(s), toTypeName(length(s)) from system.one;
select toLowCardinality('a') as val group by val order by val;
select (toLowCardinality('a') as val) || 'b' group by val order by val;
select toLowCardinality(z) as val from (select arrayJoin(['c', 'd']) as z) group by val order by val;
select (toLowCardinality(z) as val) || 'b'  from (select arrayJoin(['c', 'd']) as z) group by val order by val;

select '-';
drop table if exists lc_str_uuid;
create table lc_str_uuid(str1 String, str2 LowCardinality(String), str3 LowCardinality(String)) ENGINE=Memory;
select toUUID(str1), toUUID(str2), toUUID(str3) from lc_str_uuid;
select toUUID(str1, '', NULL), toUUID(str2, '', NULL), toUUID(str3, '', NULL) from lc_str_uuid;
insert into lc_str_uuid values ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0');
select toUUID(str1), toUUID(str2), toUUID(str3) from lc_str_uuid;
select toUUID(str1, '', NULL), toUUID(str2, '', NULL), toUUID(str3, '', NULL) from lc_str_uuid;
drop table if exists lc_str_uuid;
