drop table if exists merge;
drop table if exists merge1;
drop table if exists merge2;

set allow_deprecated_syntax_for_merge_tree=1;
create table merge1 ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into merge1 values (1, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create table merge2 ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), tuple(CounterID, StartDate, intHash32(UserID), VisitID, ClickLogID), 8192, Sign);
insert into merge2 values (2, '2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

create table merge ( CounterID UInt32,  StartDate Date,  Sign Int8,  VisitID UInt64,  UserID UInt64,  StartTime DateTime,   ClickLogID UInt64) ENGINE = Merge(currentDatabase(), 'merge\[0-9\]');

alter table merge1 add column dummy String after CounterID;
alter table merge2 add column dummy String after CounterID;
alter table merge add column dummy String after CounterID;

describe table merge;
show create table merge;

insert into merge1 values (1, 'Hello, Alter Table!','2013-09-19', 1, 0, 2, '2013-09-19 12:43:06', 3);

select CounterID, dummy from merge where dummy <> '' limit 10;


alter table merge drop column dummy;

describe table merge;
show create table merge;

--error: must correctly fall into the alter
alter table merge add column dummy1 String after CounterID;
select CounterID, dummy1 from merge where dummy1 <> '' limit 10;

drop table merge;
drop table merge1;
drop table merge2;
