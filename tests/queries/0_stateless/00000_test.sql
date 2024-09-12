-- Tags: long, no-tsan, no-msan, no-ubsan, no-asan

set allow_experimental_dynamic_type = 1;
set merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 1;

drop table if exists test;

create table test (id UInt64, d Dynamic) engine=MergeTree order by id settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, use_adaptive_write_buffer_for_dynamic_subcolumns=1, min_bytes_for_full_part_storage=100000000000;

insert into test select number, if (number % 5 == 1, ('str_' || number)::LowCardinality(String)::Dynamic, number::Dynamic) from numbers(100000) settings min_insert_block_size_rows=50000; 

select count() from test where dynamicType(d) == 'UInt64';

drop table test;
