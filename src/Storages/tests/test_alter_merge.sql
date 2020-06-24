create database if not exists test;

drop table if exists test.merge;
drop table if exists test.merge1;
drop table if exists test.merge2;

create table  test.merge1 ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into test.merge1 values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create table  test.merge2 ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into test.merge2 values (2, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create table  test.merge ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = Merge(test, 'merge\[0-9\]');

alter table test.merge1 add column dummy String after CounterID;
alter table test.merge2 add column dummy String after CounterID;
alter table test.merge add column dummy String after CounterID;

describe table test.merge;
show create table test.merge;

insert into test.merge1 values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

select CounterID, dummy from test.merge where dummy <> '' limit 10;


alter table test.merge drop column dummy;

describe table test.merge;
show create table test.merge;

--error: must correctly fall into the alter
alter table test.merge add column dummy1 String after CounterID;
select CounterID, dummy1 from test.merge where dummy1 <> '' limit 10;

