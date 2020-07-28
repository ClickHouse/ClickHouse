create database if not exists test;

drop table if exists test.merge_tree;

create table  test.merge_tree ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);

insert into test.merge_tree values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3)
alter table test.merge_tree add column dummy String after CounterID;
describe table test.merge_tree;

insert into test.merge_tree values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3)

select CounterID, dummy from test.merge_tree where dummy <> '' limit 10;

alter table test.merge_tree drop column dummy;

describe table test.merge_tree;
