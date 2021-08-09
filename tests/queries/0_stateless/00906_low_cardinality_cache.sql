drop table if exists lc_00906;
create table lc_00906 (b LowCardinality(String)) engine=MergeTree order by b;
insert into lc_00906 select '0123456789' from numbers(100000000);
select count(), b from lc_00906 group by b;
drop table if exists lc_00906;
