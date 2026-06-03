-- Tags: long

set max_threads = 3;
set min_bytes_to_use_direct_io = 1;

drop table if exists test;
create table test (id UInt64, json JSON(max_dynamic_paths=4)) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=1, index_granularity_bytes=28437532, merge_max_block_size=3520, index_granularity=26762;

system stop merges test;
insert into test select number, toJSONString(map('a', number)) from numbers(100000);
insert into test select number, toJSONString(map('b', arrayMap(x -> map('c', x), range(number % 5 + 1)))) from numbers(100000);
insert into test select number, toJSONString(map('b', arrayMap(x -> map('d', x), range(number % 5 + 1)))) from numbers(50000);

system start merges test;
optimize table test final;

select * from (select distinct json.b[] from test) order by all;

drop table test;
