-- Check remerge_sort_lowered_memory_bytes_ratio setting

set max_memory_usage='300Mi';
-- enter remerge once limit*2 is reached
set max_bytes_before_remerge_sort='10Mi';
-- more blocks
set max_block_size=40960;

-- remerge_sort_lowered_memory_bytes_ratio default 2, slightly not enough
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 819200 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 186.25 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging is not useful (memory usage was not lowered by remerge_sort_lowered_memory_bytes_ratio=2.0)
select number k, repeat(toString(number), 11) v1, repeat(toString(number), 12) v2 from numbers(3e6) order by v1, v2 limit 400e3 format Null; -- { serverError 241 }
select number k, repeat(toString(number), 11) v1, repeat(toString(number), 12) v2 from numbers(3e6) order by v1, v2 limit 400e3 settings remerge_sort_lowered_memory_bytes_ratio=2. format Null; -- { serverError 241 }

-- remerge_sort_lowered_memory_bytes_ratio 1.9 is good (need at least 1.91/0.98=1.94)
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 819200 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 186.25 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 809600 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 188.13 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 809600 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 188.13 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 809600 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 188.13 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 809600 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 188.13 MiB to 95.00 MiB
--     MergeSortingTransform: Re-merging intermediate ORDER BY data (20 blocks with 809600 rows) to save memory consumption
--     MergeSortingTransform: Memory usage is lowered from 188.13 MiB to 95.00 MiB
select number k, repeat(toString(number), 11) v1, repeat(toString(number), 12) v2 from numbers(3e6) order by k limit 400e3 settings remerge_sort_lowered_memory_bytes_ratio=1.9 format Null;
