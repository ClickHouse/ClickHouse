drop table if exists lc;
create table lc (val LowCardinality(UInt64)) engine = MergeTree order by val;
insert into lc select number % 123 from system.numbers limit 100000;
select distinct(val) from lc order by val;
drop table if exists lc;
