drop table if exists test_table;
CREATE TABLE test_table (string_value String) ENGINE = MergeTree ORDER BY string_value SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
system stop merges test_table;
insert into test_table select * from (
	select 'test_value_1'
	from numbers_mt(250000)
	union all
	select 'test_value_2'
	from numbers_mt(2000000)
)
order by rand();

select distinct
    'constant_1' as constant_value,
    count(*) over(partition by constant_value, string_value) as value_cnt
from (
    select string_value
    from test_table
)
order by all;

select distinct
 'constant_1' as constant_value, *
 from (select string_value from test_table)
 ORDER BY constant_value, string_value settings max_threads=1;

system start merges test_table;
drop table test_table;
