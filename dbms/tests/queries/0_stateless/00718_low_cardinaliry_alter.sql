create table tab (a String, b LowCardinality(UInt32)) engine = MergeTree order by a;
insert into tab values ('a', 1);
select *, toTypeName(b) from tab;
alter table tab modify column b UInt32;
select *, toTypeName(b) from tab;
alter table tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab;
alter table tab modify column b StringWithDictionary;
select *, toTypeName(b) from tab;
alter table tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab;
alter table tab modify column b String;
select *, toTypeName(b) from tab;
alter table tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from tab;
drop table if exists tab;

