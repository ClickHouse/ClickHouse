-- https://github.com/ClickHouse/ClickHouse/issues/77485
SELECT min(c0 >= ANY(SELECT '1' GROUP BY GROUPING SETS (1))) FROM (SELECT 1 c0) t0 SETTINGS group_by_use_nulls = 1;