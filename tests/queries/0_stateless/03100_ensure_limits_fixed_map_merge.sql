-- Test ensureLimitsFixedMapMerge function for parallel FixedHashMap aggregation
-- This test verifies limit enforcement after parallel merge operations


-- Throw does work, but from `executeOnBlock` not from our code.
-- Need to come up with query that not exceed threshold during execute but would hit when merges.
SELECT number % 255 as m, count(*) as cnt
FROM numbers_mt(1000)
GROUP BY m
ORDER BY m
SETTINGS max_threads = 4, max_rows_to_group_by = 10, group_by_overflow_mode = 'throw';

SELECT number % 255 as m, count(*) as cnt
FROM numbers_mt(1000)
GROUP BY m
ORDER BY m
SETTINGS max_threads = 4, max_rows_to_group_by = 10, group_by_overflow_mode = 'break';

