-- Regression test: filter pushdown through legacy JoinStep with join_use_nulls
-- caused "Unexpected return type" exception because the pushed-down filter
-- expression expected Nullable types from the join output, but the join input
-- columns are non-nullable.

SET joined_subquery_requires_alias = 0;
SET any_join_distinct_right_table_keys = 1;
SET join_use_nulls = 1;
SET enable_analyzer = 0;

SELECT k, x, y
FROM (SELECT arrayJoin([1, 2, 3]) AS k, 'Hello' AS x)
ANY RIGHT JOIN (SELECT range(k) AS y, arrayJoin([3, 4, 5]) AS k) USING k
WHERE k < 10
ORDER BY k;
