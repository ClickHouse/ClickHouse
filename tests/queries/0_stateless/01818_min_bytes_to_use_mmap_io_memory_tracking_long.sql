drop table if exists data_01818;

create table data_01818 (key Int, value String) engine=MergeTree() order by key settings min_bytes_for_wide_part=0 as select number, randomPrintableASCII(100) from numbers(1e6);
select * from data_01818 format Null settings min_bytes_to_use_mmap_io=1, max_memory_usage='20Mi', max_threads=1; -- { serverError 241 }
select * from data_01818 format Null settings min_bytes_to_use_mmap_io=1e9, max_memory_usage='20Mi', max_threads=1;
