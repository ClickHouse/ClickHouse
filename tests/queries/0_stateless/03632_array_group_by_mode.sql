-- Tests for array_group_by_mode setting

-- Test default (ordered) mode - [1,2,3] and [3,2,1] are different groups
SELECT 'ordered mode (default):';
SELECT arr, count() AS c FROM (
    SELECT [1, 2, 3] AS arr
    UNION ALL SELECT [3, 2, 1] AS arr
    UNION ALL SELECT [1, 2, 3] AS arr
) GROUP BY arr ORDER BY arr;

-- Test multiset mode - [1,2,3] and [3,2,1] are the same group, but [1,1,2] and [1,2,2] are different
SELECT 'multiset mode:';
SET array_group_by_mode = 'multiset';
SELECT arr, count() AS c FROM (
    SELECT [1, 2, 3] AS arr
    UNION ALL SELECT [3, 2, 1] AS arr
    UNION ALL SELECT [1, 2, 3] AS arr
) GROUP BY arr ORDER BY arr;

SELECT 'multiset mode with duplicates:';
SELECT arr, count() AS c FROM (
    SELECT [1, 1, 2] AS arr
    UNION ALL SELECT [1, 2, 1] AS arr
    UNION ALL SELECT [1, 2, 2] AS arr
) GROUP BY arr ORDER BY arr;

-- Test set mode - [1,2,3], [3,2,1], and [1,1,2,3] are all the same group
SELECT 'set mode:';
SET array_group_by_mode = 'set';
SELECT arr, count() AS c FROM (
    SELECT [1, 2, 3] AS arr
    UNION ALL SELECT [3, 2, 1] AS arr
    UNION ALL SELECT [1, 2, 3] AS arr
) GROUP BY arr ORDER BY arr;

SELECT 'set mode with duplicates:';
SELECT arr, count() AS c FROM (
    SELECT [1, 1, 2] AS arr
    UNION ALL SELECT [1, 2, 1] AS arr
    UNION ALL SELECT [1, 2, 2] AS arr
    UNION ALL SELECT [1, 2] AS arr
) GROUP BY arr ORDER BY arr;

-- Test empty arrays
SELECT 'empty arrays:';
SET array_group_by_mode = 'multiset';
SELECT arr, count() AS c FROM (
    SELECT [] AS arr
    UNION ALL SELECT [] AS arr
) GROUP BY arr;

-- Test with GROUP BY ALL
SELECT 'multiset with GROUP BY ALL:';
SET array_group_by_mode = 'multiset';
SELECT arr, count() AS c FROM (
    SELECT [1, 2, 3] AS arr
    UNION ALL SELECT [3, 2, 1] AS arr
) GROUP BY ALL ORDER BY arr;

-- Test with string arrays
SELECT 'string arrays in multiset mode:';
SET array_group_by_mode = 'multiset';
SELECT arr, count() AS c FROM (
    SELECT ['a', 'b', 'c'] AS arr
    UNION ALL SELECT ['c', 'b', 'a'] AS arr
    UNION ALL SELECT ['a', 'b', 'c'] AS arr
) GROUP BY arr ORDER BY arr;

-- Reset to default
SET array_group_by_mode = 'ordered';
