drop table if exists test.lc;
create table test.lc (b LowCardinality(String)) engine=MergeTree order by b;
insert into test.lc select '0123456789' from numbers(100000000);
select count(), b from test.lc group by b;
drop table if exists test.lc;
