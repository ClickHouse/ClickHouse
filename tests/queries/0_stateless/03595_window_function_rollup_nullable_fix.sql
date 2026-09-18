-- Test for issue #85465: Window functions with ROLLUP and group_by_use_nulls
-- This tests the fix for nullable/non-nullable column compatibility in window functions

SET enable_analyzer = 1;

-- Test the exact failing case from the original issue  
SELECT * FROM (SELECT 1 x, lag(1) OVER () as lag_val GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1) ORDER BY x;

-- Test with lead function
SELECT * FROM (SELECT 1 x, lead(1) OVER () as lead_val GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1) ORDER BY x;

-- Test with different data types - String
SELECT * FROM (SELECT 'test' x, lag('value') OVER () as lag_val GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1) ORDER BY x;

-- Test with Float64
SELECT * FROM (SELECT 1.5 x, lag(2.5) OVER () as lag_val GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1) ORDER BY x;

-- Test with simple integer values
SELECT * FROM (SELECT 2 x, lag(10) OVER () as lag_val GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls = 1) ORDER BY x;