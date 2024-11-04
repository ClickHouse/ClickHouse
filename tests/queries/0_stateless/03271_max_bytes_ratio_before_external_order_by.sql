-- prefer_external_sort_block_bytes is crucial for performance of this test
set max_threads=1, max_bytes_before_external_sort=0, max_memory_usage='400Mi', max_block_size=65535, prefer_external_sort_block_bytes=16744704, max_rows_to_read=0;
select number from numbers(100e6) order by number format Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
select number from numbers(100e6) order by number settings max_bytes_ratio_before_external_sort=0.5 format Null;
