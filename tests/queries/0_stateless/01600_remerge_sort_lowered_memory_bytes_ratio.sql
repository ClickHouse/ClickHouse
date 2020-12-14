-- Check remerge_sort_lowered_memory_bytes_ratio setting

set max_memory_usage='4Gi';
-- enter remerge once limit*2 is reached
set max_bytes_before_remerge_sort='10Mi';
-- more blocks
set max_block_size=40960;

-- default 2, slightly not enough:
--
--     MergeSortingTransform: Memory usage is lowered from 1.91 GiB to 980.00 MiB
--     MergeSortingTransform: Re-merging is not useful (memory usage was not lowered by remerge_sort_lowered_memory_bytes_ratio=2.0)
select number k, repeat(toString(number), 101) v1, repeat(toString(number), 102) v2, repeat(toString(number), 103) v3 from numbers(toUInt64(10e6)) order by k limit 400e3 format Null; -- { serverError 241 }
select number k, repeat(toString(number), 101) v1, repeat(toString(number), 102) v2, repeat(toString(number), 103) v3 from numbers(toUInt64(10e6)) order by k limit 400e3 settings remerge_sort_lowered_memory_bytes_ratio=2. format Null; -- { serverError 241 }

-- 1.91/0.98=1.94 is good
select number k, repeat(toString(number), 101) v1, repeat(toString(number), 102) v2, repeat(toString(number), 103) v3 from numbers(toUInt64(10e6)) order by k limit 400e3 settings remerge_sort_lowered_memory_bytes_ratio=1.9 format Null;
