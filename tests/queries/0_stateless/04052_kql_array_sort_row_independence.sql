-- Regression test: kql_array_sort_asc/desc should process each row independently.
-- Previously, mismatched array lengths in one row could affect other rows because
-- hasEqualOffsets was a column-wide check.

-- The bug: this outputs ([],[NULL]) for the first row, but if you leave only the
-- first row (remove ", [1]"), it outputs ([],[]).
SELECT kql_array_sort_desc(a, []::Array(Int8)) FROM system.one ARRAY JOIN CAST([[], [1]] AS Array(Array(UInt64))) AS a;

-- Same test for ascending
SELECT kql_array_sort_asc(a, []::Array(Int8)) FROM system.one ARRAY JOIN CAST([[], [1]] AS Array(Array(UInt64))) AS a;

-- Single row should produce the same result as the corresponding row in multi-row query
SELECT kql_array_sort_desc([]::Array(UInt64), []::Array(Int8));
SELECT kql_array_sort_desc([1]::Array(UInt64), []::Array(Int8));

-- Multiple arrays with varying lengths across rows
SELECT kql_array_sort_asc(a, b) FROM (SELECT [3, 1, 2] AS a, [10, 20, 30] AS b UNION ALL SELECT [5, 4] AS a, [100] AS b) ORDER BY length(a);
