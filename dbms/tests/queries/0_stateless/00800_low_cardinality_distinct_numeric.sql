drop table if exists lc_00800_2;
create table lc_00800_2 (val LowCardinality(UInt64)) engine = MergeTree order by val;
insert into lc_00800_2 select number % 123 from system.numbers limit 100000;
select distinct(val) from lc_00800_2 order by val;
drop table if exists lc_00800_2
;
