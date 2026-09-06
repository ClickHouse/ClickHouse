-- Tags: no-old-analyzer
-- no-old-analyzer: `eval` requires the analyzer.

-- The generated query is analyzed with the generated query's own SETTINGS, so its set operation modes
-- and AST size limits behave the same as when the query is executed directly.
-- See https://github.com/ClickHouse/ClickHouse/pull/110211

SET allow_experimental_eval_table_function = 1;

-- The set operation modes are resolved from the generated query's own SETTINGS, so an inner
-- `union_default_mode = 'DISTINCT'` normalizes an ambiguous UNION the same way it would when the
-- query is executed directly, even though the outer default would reject it.
SELECT count() FROM eval('SELECT 1 AS n UNION SELECT 1 AS n SETTINGS union_default_mode = ''DISTINCT''');
SELECT count() FROM eval('SELECT 1 AS n UNION SELECT 1 AS n'); -- { serverError EXPECTED_ALL_OR_DISTINCT }

-- INTERSECT / EXCEPT default modes are likewise resolved from the generated query's own SETTINGS.
-- These modes live in separate branches of `SelectIntersectExceptQueryVisitor`, so the inner
-- `intersect_default_mode` / `except_default_mode = 'ALL'` must win over the outer `'DISTINCT'`,
-- keeping duplicates exactly as when the query is executed directly. INTERSECT ALL of [1,1,2] and
-- [1,1,3] keeps two 1s; EXCEPT ALL of [1,1,1,2] and [1] keeps 1,1,2.
SELECT count() FROM eval('SELECT arrayJoin([1, 1, 2]) AS n INTERSECT SELECT arrayJoin([1, 1, 3]) AS n SETTINGS intersect_default_mode = ''ALL''') SETTINGS intersect_default_mode = 'DISTINCT';
SELECT count() FROM eval('SELECT arrayJoin([1, 1, 1, 2]) AS n EXCEPT SELECT arrayJoin([1]) AS n SETTINGS except_default_mode = ''ALL''') SETTINGS except_default_mode = 'DISTINCT';

-- The AST size limits `max_ast_elements` / `max_ast_depth` apply to the generated query, same as when
-- it is executed directly, so a tiny outer query cannot smuggle a huge or deep AST past them.
SELECT count() FROM eval('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10') SETTINGS max_ast_elements = 30; -- { serverError TOO_BIG_AST }
SELECT count() FROM eval('SELECT (((((1)))))') SETTINGS max_ast_depth = 3; -- { serverError TOO_DEEP_AST }
-- The limits are read after the generated query's own SETTINGS are applied, so an inner SETTINGS clause
-- controls its own AST size limits, both to tighten and to relax them.
SELECT count() FROM eval('SELECT 1 + 2 + 3 + 4 + 5 SETTINGS max_ast_elements = 5'); -- { serverError TOO_BIG_AST }
SELECT * FROM eval('SELECT 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10 AS big SETTINGS max_ast_elements = 100000') SETTINGS max_ast_elements = 30;
-- The size limits are checked after the global `WITH` aliases are expanded (as in a direct query), so a
-- global CTE that stays small before expansion but grows past `max_ast_elements` once it is inlined into
-- every UNION branch is rejected, instead of slipping through the pre-expansion check.
SELECT count() FROM eval('WITH 1+2+3+4+5+6+7+8+9+10+11+12+13+14+15 AS big SELECT big UNION ALL SELECT big UNION ALL SELECT big UNION ALL SELECT big UNION ALL SELECT big UNION ALL SELECT big UNION ALL SELECT big UNION ALL SELECT big SETTINGS max_ast_elements = 100'); -- { serverError TOO_BIG_AST }

-- `enforce_strict_identifier_format` applies to the generated query, same as when it is executed
-- directly, so moving the query text into `eval` does not get a rejected identifier accepted.
SELECT * FROM eval('SELECT 1 AS "bad-name"') SETTINGS enforce_strict_identifier_format = 1; -- { serverError BAD_ARGUMENTS }
-- It is read after the generated query's own SETTINGS are applied, so an inner clause enables it.
SELECT * FROM eval('SELECT 1 AS "bad-name" SETTINGS enforce_strict_identifier_format = 1') SETTINGS enforce_strict_identifier_format = 0; -- { serverError BAD_ARGUMENTS }
-- It is checked after the construction settings are materialized, so an identifier that only the
-- generated query's own `filter` introduces is rejected as well.
SELECT * FROM eval('SELECT 1 AS good SETTINGS filter = ''"bad-col" = 1''') SETTINGS enforce_strict_identifier_format = 1; -- { serverError BAD_ARGUMENTS }
-- An alphanumeric identifier is still accepted with the check enabled.
SELECT * FROM eval('SELECT 1 AS good_name') SETTINGS enforce_strict_identifier_format = 1;
