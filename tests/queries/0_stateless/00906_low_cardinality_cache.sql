-- Tags: long

SET max_rows_to_read = '100M';
drop table if exists lc_00906;
create table lc_00906 (b LowCardinality(String)) engine=MergeTree order by b SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into lc_00906 select '0123456789' from numbers(100000000);
select count(), b from lc_00906 group by b;
drop table if exists lc_00906;
