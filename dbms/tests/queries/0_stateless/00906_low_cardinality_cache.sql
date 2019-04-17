drop table if exists lc;
create table lc (b LowCardinality(String)) engine=MergeTree order by b;
insert into lc select '0123456789' from numbers(100000000);
select count(), b from lc group by b;
drop table if exists lc;
