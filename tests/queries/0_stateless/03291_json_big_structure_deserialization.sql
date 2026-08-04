set enable_json_type=1;

create table test (json JSON(max_dynamic_paths=0)) engine=MergeTree order by tuple() settings max_compress_block_size = 128, marks_compress_block_size=128, min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, index_granularity = 8192, replace_long_file_name_to_hash=1;
insert into test select toJSONString(map(repeat('a' || number, 5000), 42)) from numbers(10000);

set max_threads=1;
set enable_filesystem_cache=0;
set max_parallel_replicas=1;
set remote_filesystem_read_method='read';
set remote_filesystem_read_prefetch=0;

select json.a from test format Null;
drop table test;
