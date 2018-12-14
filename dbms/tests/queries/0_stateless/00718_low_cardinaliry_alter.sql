set allow_experimental_low_cardinality_type = 1;

create table test.tab (a String, b LowCardinality(UInt32)) engine = MergeTree order by a;
insert into test.tab values ('a', 1);
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b UInt32;
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b StringWithDictionary;
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b String;
select *, toTypeName(b) from test.tab;
alter table test.tab modify column b LowCardinality(UInt32);
select *, toTypeName(b) from test.tab;
drop table if exists test.tab;

