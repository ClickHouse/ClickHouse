drop table if exists test;
create table test (t Tuple(a UInt32), a UInt32 materialized t.a) engine=MergeTree() order by tuple();
insert into test select tuple(1);
select t, a from test;
alter table test update t = tuple(2) where 1 settings mutations_sync=1;
select t, a from test;
alter table test update t = tuple(3) where 1 settings mutations_sync=1;
select t, a from test;
drop table test;

drop table if exists test;
create table test (t Tuple(a UInt32), a UInt32 materialized t.a + 42) engine=MergeTree() order by tuple();
insert into test select tuple(1);
select t, a from test;
alter table test update t = tuple(2) where 1 settings mutations_sync=1;
select t, a from test;
alter table test update t = tuple(3) where 1 settings mutations_sync=1;
select t, a from test;
drop table test;


create table test (t Tuple(a UInt32), a UInt32 materialized t.a) engine=MergeTree() order by a;
insert into test select tuple(1);
select t, a from test;
alter table test update t = tuple(2) where 1 settings mutations_sync=1; -- {serverError CANNOT_UPDATE_COLUMN}
drop table test;

drop table if exists test;
create table test (json JSON, a UInt32 materialized json.a::UInt32) engine=MergeTree() order by tuple();
insert into test select '{"a" : 1}';
select json, a from test;
alter table test update json = '{"a" : 2}' where 1 settings mutations_sync=1;
select json, a from test;
alter table test update json = '{"a" : 3}' where 1 settings mutations_sync=1;
select json, a from test;
drop table test;

drop table if exists test;
create table test (json JSON, a UInt32 materialized json.a::UInt32 + 42) engine=MergeTree() order by tuple();
insert into test select '{"a" : 1}';
select json, a from test;
alter table test update json = '{"a" : 2}' where 1 settings mutations_sync=1;
select json, a from test;
alter table test update json = '{"a" : 3}' where 1 settings mutations_sync=1;
select json, a from test;
drop table test;



create table test (json JSON, a UInt32 materialized json.a::UInt32) engine=MergeTree() order by a;
insert into test select '{"a" : 1}';
select json, a from test;
alter table test update json = '{"a" : 2}' where 1 settings mutations_sync=1; -- {serverError CANNOT_UPDATE_COLUMN}
drop table test;

