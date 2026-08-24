SELECT 'a' AS key, 'b' as value GROUP BY key WITH CUBE SETTINGS group_by_use_nulls = 1;
