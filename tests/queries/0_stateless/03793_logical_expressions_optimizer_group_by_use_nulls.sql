-- In this case, we optimize `0 = (...)` to `NOT (...)`, but we have to keep Nullable as needed for `group_by_use_nulls`:
SELECT 0 = (2 = dummy) GROUP BY GROUPING SETS ((1)) SETTINGS group_by_use_nulls = 1;
