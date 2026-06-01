-- Test the `sql_compatibility_mode` supersetting.

SELECT '--- default value ---';
SELECT getSetting('sql_compatibility_mode');

SELECT '--- standard mode flips all curated settings ---';
SET sql_compatibility_mode = 'standard';
SELECT getSetting('sql_compatibility_mode');
SELECT getSetting('data_type_default_nullable');
SELECT getSetting('group_by_use_nulls');
SELECT getSetting('intersect_default_mode');
SELECT getSetting('except_default_mode');
SELECT getSetting('joined_subquery_requires_alias');
SELECT getSetting('join_use_nulls');
SELECT getSetting('union_default_mode');
SELECT getSetting('aggregate_functions_null_for_empty');
SELECT getSetting('cast_keep_nullable');
SELECT getSetting('prefer_column_name_to_alias');

SELECT '--- revert to default restores originals ---';
SET sql_compatibility_mode = 'default';
SELECT getSetting('data_type_default_nullable');
SELECT getSetting('group_by_use_nulls');
SELECT getSetting('intersect_default_mode');
SELECT getSetting('except_default_mode');
SELECT getSetting('joined_subquery_requires_alias');
SELECT getSetting('join_use_nulls');
SELECT getSetting('union_default_mode');
SELECT getSetting('aggregate_functions_null_for_empty');
SELECT getSetting('cast_keep_nullable');
SELECT getSetting('prefer_column_name_to_alias');

SELECT '--- manual override before mode switch wins ---';
SET join_use_nulls = 0;
SET sql_compatibility_mode = 'standard';
SELECT getSetting('join_use_nulls');         -- still 0
SELECT getSetting('group_by_use_nulls');     -- 1 (mode applied)

SELECT '--- manual change after mode switch is preserved on revert ---';
SET sql_compatibility_mode = 'default';
SET join_use_nulls = 0;
SET sql_compatibility_mode = 'standard';
SET cast_keep_nullable = 0;
SET sql_compatibility_mode = 'default';
SELECT getSetting('cast_keep_nullable');     -- 0, user value wins
SELECT getSetting('group_by_use_nulls');     -- 0, reverted to default

SELECT '--- normal SQL still works under standard mode ---';
SET sql_compatibility_mode = 'default';
SET join_use_nulls = DEFAULT, cast_keep_nullable = DEFAULT;
SET sql_compatibility_mode = 'standard';

SELECT 1 + 1;
SELECT sum(number) FROM numbers(5);
SELECT upper('clickhouse');
SELECT arraySum([1, 2, 3]);
SELECT x FROM (SELECT 42 AS x) AS t;
SELECT number FROM numbers(5) WHERE number > 2 ORDER BY number;
SELECT k, count() FROM (SELECT number % 2 AS k FROM numbers(6)) AS t GROUP BY k ORDER BY k;
SELECT number FROM numbers(5) ORDER BY number DESC LIMIT 2;

SELECT '--- per-setting behavioral: join_use_nulls ---';
SET sql_compatibility_mode = 'default';
SELECT b, toTypeName(b) FROM (SELECT 1 AS a) AS t1 LEFT JOIN (SELECT 2 AS a, 3 AS b) AS t2 USING (a);
SET sql_compatibility_mode = 'standard';
SELECT b, toTypeName(b) FROM (SELECT 1 AS a) AS t1 LEFT JOIN (SELECT 2 AS a, 3 AS b) AS t2 USING (a);

SELECT '--- per-setting behavioral: union_default_mode ---';
SET sql_compatibility_mode = 'default';
SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 AS x) AS t ORDER BY x;
SET sql_compatibility_mode = 'standard';
SELECT * FROM (SELECT 1 AS x UNION SELECT 1 AS x) AS t ORDER BY x;

SELECT '--- per-setting behavioral: intersect_default_mode ---';
-- Duplicates on both sides so ALL vs DISTINCT differ.
SET sql_compatibility_mode = 'default';
SELECT arrayJoin([1, 1, 2]) AS x INTERSECT SELECT arrayJoin([1, 1, 2]) AS x ORDER BY x;
SET sql_compatibility_mode = 'standard';
SELECT arrayJoin([1, 1, 2]) AS x INTERSECT SELECT arrayJoin([1, 1, 2]) AS x ORDER BY x;

SELECT '--- per-setting behavioral: except_default_mode ---';
SET sql_compatibility_mode = 'default';
SELECT arrayJoin([1, 1, 2]) AS x EXCEPT SELECT 2 ORDER BY x;
SET sql_compatibility_mode = 'standard';
SELECT arrayJoin([1, 1, 2]) AS x EXCEPT SELECT 2 ORDER BY x;

SELECT '--- per-setting behavioral: aggregate_functions_null_for_empty ---';
SET sql_compatibility_mode = 'default';
SELECT sum(x) AS s, toTypeName(s) FROM (SELECT 1 AS x WHERE 0);
SET sql_compatibility_mode = 'standard';
SELECT sum(x) AS s, toTypeName(s) FROM (SELECT 1 AS x WHERE 0);

SELECT '--- per-setting behavioral: cast_keep_nullable ---';
SET sql_compatibility_mode = 'default';
SELECT toTypeName(CAST(materialize(toNullable(1)) AS UInt32));
SET sql_compatibility_mode = 'standard';
SELECT toTypeName(CAST(materialize(toNullable(1)) AS UInt32));

SELECT '--- per-setting behavioral: data_type_default_nullable ---';
SET sql_compatibility_mode = 'default';
CREATE TEMPORARY TABLE t_dtdn_default (x UInt32) ENGINE = Memory;
SELECT type FROM system.columns WHERE table = 't_dtdn_default' AND name = 'x';
SET sql_compatibility_mode = 'standard';
CREATE TEMPORARY TABLE t_dtdn_standard (x UInt32) ENGINE = Memory;
SELECT type FROM system.columns WHERE table = 't_dtdn_standard' AND name = 'x';

SELECT '--- per-setting behavioral: group_by_use_nulls ---';
SET sql_compatibility_mode = 'default';
SELECT k FROM (SELECT 1 AS k) AS t GROUP BY ROLLUP(k) ORDER BY k;
SET sql_compatibility_mode = 'standard';
SELECT k FROM (SELECT 1 AS k) AS t GROUP BY ROLLUP(k) ORDER BY k NULLS LAST;

SELECT '--- per-setting behavioral: prefer_column_name_to_alias ---';
-- numbers(11) where number > 1 = {2..10}. Alias `number` shadows source column.
-- default mode: ORDER BY uses alias (String) -> lexicographic '10','2','3'...
-- standard mode: ORDER BY uses original UInt64 -> 2,3,4...
SET sql_compatibility_mode = 'default';
SELECT toString(number) AS number FROM numbers(11) WHERE number > 1 ORDER BY number LIMIT 3;
SET sql_compatibility_mode = 'standard';
SELECT toString(number) AS number FROM numbers(11) WHERE number > 1 ORDER BY number LIMIT 3;

SELECT '--- invalid value rejected ---';
SET sql_compatibility_mode = 'nonsense'; -- { serverError BAD_ARGUMENTS }
