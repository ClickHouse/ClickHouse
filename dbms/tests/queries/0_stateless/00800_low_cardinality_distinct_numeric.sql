drop table if exists test.lc;
create table test.lc (val LowCardinality(UInt64)) engine = MergeTree order by val;
insert into test.lc select number % 123 from system.numbers limit 100000;
select distinct(val) from test.lc order by val;
drop table if exists test.lc;
