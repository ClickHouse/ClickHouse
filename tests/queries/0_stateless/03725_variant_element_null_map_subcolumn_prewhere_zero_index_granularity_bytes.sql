drop table if exists test;
create table test (s Int128, v Variant(UUID, Int128)) engine=MergeTree order by s settings index_granularity = 2, index_granularity_bytes = 0, min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
insert into test select 42::Int128, 42::Int128;
select v from test prewhere 1;
drop table test;

