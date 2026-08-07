drop table if exists test;
create table test (id UInt32, t Tuple(a UInt32)) engine=MergeTree order by t.a;
insert into test select 1, tuple(1);
alter table test update t = tuple(2) where 1; -- {serverError CANNOT_UPDATE_COLUMN}
alter table test modify column t Tuple(a String); -- {serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
drop table test;

drop table if exists test;
create table test (id UInt32, json JSON) engine=MergeTree order by json.a::Int64;
insert into test select 1, '{"a" : 42}';
alter table test update json = '{}' where 1; -- {serverError CANNOT_UPDATE_COLUMN}
alter table test modify column json JSON(a String); -- {serverError ALTER_OF_COLUMN_IS_FORBIDDEN}
drop table test;

