-- https://github.com/ClickHouse/ClickHouse/issues/105916
-- `ASTSelectIntersectExceptQuery::formatImpl` must wrap compound children
-- (`ASTSelectWithUnionQuery` or a nested `ASTSelectIntersectExceptQuery`) in
-- parentheses, otherwise the lower-precedence `UNION` (or the nested set operator)
-- gets reparsed as part of the outer query and the view returns a wrong result
-- after `DETACH`/`ATTACH`.
--
-- Each case checks two things:
--  1. The rows match before and after a `DETACH`/`ATTACH` roundtrip.
--  2. The stored SQL pins the exact parenthesized form. The rows can coincide
--     between the correct and the mis-parenthesized form on small `{1,2}`-style
--     sets, so the rows-only check is not enough on its own. `formatQuerySingleLine`
--     + `extract(..., 'AS .*')` keeps the assertion stable across the database
--     name, the table UUID and column-list formatting.

-- Case 1: `EXCEPT` with `UNION ALL` on the right.
-- {2} EXCEPT {2, 1} = {} (empty)
-- Without the fix: re-parses as ({2} EXCEPT {2}) UNION ALL {1} = {1}.
DROP TABLE IF EXISTS v_except_union_right;

CREATE VIEW v_except_union_right AS SELECT 2 c0 EXCEPT (SELECT 2 UNION ALL SELECT 1);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_union_right';
SELECT * FROM v_except_union_right ORDER BY all;
SELECT '---';

DETACH TABLE v_except_union_right SYNC;
ATTACH TABLE v_except_union_right;

SELECT * FROM v_except_union_right ORDER BY all;

DROP TABLE v_except_union_right;
SELECT '===';

-- Case 2: `INTERSECT` with `UNION ALL` on the right.
-- {1} INTERSECT {1, 2} = {1}
-- Without the fix: re-parses as ({1} INTERSECT {1}) UNION ALL {2} = {1, 2}.
DROP TABLE IF EXISTS v_intersect_union_right;

CREATE VIEW v_intersect_union_right AS SELECT 1 c0 INTERSECT (SELECT 1 UNION ALL SELECT 2);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_intersect_union_right';
SELECT * FROM v_intersect_union_right ORDER BY all;
SELECT '---';

DETACH TABLE v_intersect_union_right SYNC;
ATTACH TABLE v_intersect_union_right;

SELECT * FROM v_intersect_union_right ORDER BY all;

DROP TABLE v_intersect_union_right;
SELECT '===';

-- Case 3: `EXCEPT` with `UNION ALL` on the left.
-- {1, 2} EXCEPT {1} = {2}
-- Without the fix: re-parses as {1} UNION ALL ({2} EXCEPT {1}) = {1, 2}.
DROP TABLE IF EXISTS v_except_union_left;

CREATE VIEW v_except_union_left AS (SELECT 1 UNION ALL SELECT 2) EXCEPT SELECT 1;

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_union_left';
SELECT * FROM v_except_union_left ORDER BY all;
SELECT '---';

DETACH TABLE v_except_union_left SYNC;
ATTACH TABLE v_except_union_left;

SELECT * FROM v_except_union_left ORDER BY all;

DROP TABLE v_except_union_left;
SELECT '===';

-- Case 4: `EXCEPT DISTINCT` (explicit mode) with `UNION ALL` on the right.
DROP TABLE IF EXISTS v_except_distinct_union;

CREATE VIEW v_except_distinct_union AS SELECT 2 c0 EXCEPT DISTINCT (SELECT 2 UNION ALL SELECT 1);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_distinct_union';
SELECT * FROM v_except_distinct_union ORDER BY all;
SELECT '---';

DETACH TABLE v_except_distinct_union SYNC;
ATTACH TABLE v_except_distinct_union;

SELECT * FROM v_except_distinct_union ORDER BY all;

DROP TABLE v_except_distinct_union;
SELECT '===';

-- Case 5: `INTERSECT DISTINCT` (explicit mode) with `UNION ALL` on the right.
DROP TABLE IF EXISTS v_intersect_distinct_union;

CREATE VIEW v_intersect_distinct_union AS SELECT 1 c0 INTERSECT DISTINCT (SELECT 1 UNION ALL SELECT 2);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_intersect_distinct_union';
SELECT * FROM v_intersect_distinct_union ORDER BY all;
SELECT '---';

DETACH TABLE v_intersect_distinct_union SYNC;
ATTACH TABLE v_intersect_distinct_union;

SELECT * FROM v_intersect_distinct_union ORDER BY all;

DROP TABLE v_intersect_distinct_union;
SELECT '===';

-- Case 6: `UNION DISTINCT` on the right.
-- {2} EXCEPT {2, 1} (distinct) = {}
DROP TABLE IF EXISTS v_except_union_distinct;

CREATE VIEW v_except_union_distinct AS SELECT 2 c0 EXCEPT (SELECT 2 UNION DISTINCT SELECT 1);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_union_distinct';
SELECT * FROM v_except_union_distinct ORDER BY all;
SELECT '---';

DETACH TABLE v_except_union_distinct SYNC;
ATTACH TABLE v_except_union_distinct;

SELECT * FROM v_except_union_distinct ORDER BY all;

DROP TABLE v_except_union_distinct;
SELECT '===';

-- Case 7: `INTERSECT` with `UNION ALL` on the left (symmetric counterpart of case 3).
-- {1, 2} INTERSECT {1} = {1}. INTERSECT and EXCEPT build the left operand on
-- different branches in `SelectIntersectExceptQueryMatcher`, so the EXCEPT-left
-- case (case 3) does not cover this. The rows here coincide with the
-- mis-parenthesized form ({1} UNION ALL ({2} INTERSECT {1}) = {1}), so the SQL
-- pin is what actually guards the parenthesization.
DROP TABLE IF EXISTS v_intersect_union_left;

CREATE VIEW v_intersect_union_left AS (SELECT 1 UNION ALL SELECT 2) INTERSECT SELECT 1;

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_intersect_union_left';
SELECT * FROM v_intersect_union_left ORDER BY all;
SELECT '---';

DETACH TABLE v_intersect_union_left SYNC;
ATTACH TABLE v_intersect_union_left;

SELECT * FROM v_intersect_union_left ORDER BY all;

DROP TABLE v_intersect_union_left;
SELECT '===';

-- Case 8: nested `INTERSECT` child of an `EXCEPT`, exercising the second
-- `need_parens` branch (a direct `ASTSelectIntersectExceptQuery` child, not
-- wrapped in `ASTSelectWithUnionQuery`). `SELECT 1 EXCEPT SELECT 2 INTERSECT
-- SELECT 3` folds to `{1} EXCEPT ({2} INTERSECT {3})` because INTERSECT binds
-- tighter than EXCEPT. {2} INTERSECT {3} = {}, so the result is {1}.
DROP TABLE IF EXISTS v_except_nested_intersect;

CREATE VIEW v_except_nested_intersect AS SELECT 1 c0 EXCEPT SELECT 2 INTERSECT SELECT 3;

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_nested_intersect';
SELECT * FROM v_except_nested_intersect ORDER BY all;
SELECT '---';

DETACH TABLE v_except_nested_intersect SYNC;
ATTACH TABLE v_except_nested_intersect;

SELECT * FROM v_except_nested_intersect ORDER BY all;

DROP TABLE v_except_nested_intersect;
SELECT '===';

-- Case 9: explicit nested `INTERSECT` on the right of `EXCEPT`.
-- {1} EXCEPT ({1} INTERSECT {2} = {}) = {1}.
DROP TABLE IF EXISTS v_except_paren_intersect;

CREATE VIEW v_except_paren_intersect AS SELECT 1 c0 EXCEPT (SELECT 1 INTERSECT SELECT 2);

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_paren_intersect';
SELECT * FROM v_except_paren_intersect ORDER BY all;
SELECT '---';

DETACH TABLE v_except_paren_intersect SYNC;
ATTACH TABLE v_except_paren_intersect;

SELECT * FROM v_except_paren_intersect ORDER BY all;

DROP TABLE v_except_paren_intersect;
SELECT '===';

-- Case 10: longer mixed chain combining `UNION ALL`, `EXCEPT` and a nested
-- parenthesized `EXCEPT`/`UNION ALL` subquery. Exercises both `need_parens`
-- branches at multiple nesting levels.
-- ({1,2} EXCEPT (({2} EXCEPT {1}) UNION ALL {1} = {2,1})) EXCEPT {4} = {}.
DROP TABLE IF EXISTS v_mixed_chain;

CREATE VIEW v_mixed_chain AS SELECT 1 UNION ALL SELECT 2 EXCEPT (SELECT 2 EXCEPT SELECT 1 UNION ALL SELECT 1) EXCEPT SELECT 4;

SELECT extract(formatQuerySingleLine(create_table_query), 'AS .*') FROM system.tables WHERE database = currentDatabase() AND name = 'v_mixed_chain';
SELECT * FROM v_mixed_chain ORDER BY all;
SELECT '---';

DETACH TABLE v_mixed_chain SYNC;
ATTACH TABLE v_mixed_chain;

SELECT * FROM v_mixed_chain ORDER BY all;

DROP TABLE v_mixed_chain;
