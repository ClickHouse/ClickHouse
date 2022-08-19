-- Tags: no-replicated-database

drop table if exists data_01641;

-- Disable cache for s3 storage tests because it increases memory usage.
set enable_filesystem_cache=0;
set remote_filesystem_read_method='read';

create table data_01641 (key Int, value String) engine=MergeTree order by (key, repeat(value, 40)) settings old_parts_lifetime=0, min_bytes_for_wide_part=0;

SET max_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
insert into data_01641 select number, toString(number) from numbers(120000);

-- Definitely should fail and it proves that memory is tracked in OPTIMIZE query.
set max_memory_usage='10Mi', max_untracked_memory=0;

optimize table data_01641 final; -- { serverError 241 }

drop table data_01641;
