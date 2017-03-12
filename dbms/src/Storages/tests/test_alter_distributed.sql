create database if not exists test;

drop table if exists test.merge_distributed;
drop table if exists test.merge_distributed1;

create table  test.merge_distributed1 ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into test.merge_distributed1 values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create table  test.merge_distributed ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = Distributed(self, test, merge_distributed1);

alter table test.merge_distributed1 add column dummy String after CounterID;
alter table test.merge_distributed add column dummy String after CounterID;

describe table test.merge_distributed;
show create table test.merge_distributed;

insert into test.merge_distributed1 values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);
select CounterID, dummy from test.merge_distributed where dummy <> '' limit 10;

alter table test.merge_distributed drop column dummy;

describe table test.merge_distributed;
show create table test.merge_distributed;

--error: should fall, because there is no `dummy1` column
alter table test.merge_distributed add column dummy1 String after CounterID;
select CounterID, dummy1 from test.merge_distributed where dummy1 <> '' limit 10;

