-- Related: https://github.com/ClickHouse/ClickHouse/pull/107582
-- `ASTSelectWithUnionQuery::formatQueryImpl` must wrap an
-- `ASTSelectIntersectExceptQuery` child of a UNION chain in parentheses. After
-- `SelectIntersectExceptQueryVisitor` runs, the INTERSECT/EXCEPT mode no longer
-- lives in `list_of_modes`, so the mode-based check cannot see it, and the other
-- check matches only `ASTSelectWithUnionQuery`, which this node is not. The child
-- was emitted bare, so the grouping moved on re-parse:
--   `A UNION ALL (B EXCEPT C)`  came back as  `(A UNION ALL B) EXCEPT C`.
--
-- Each case checks two things:
--  1. The stored SQL pins the exact parenthesized form. Rows can coincide
--     between the correct and the mis-parenthesized form (INTERSECT binds
--     tighter than UNION, so it survives on results alone), which makes the
--     SQL pin the real guard.
--  2. The rows match before and after a `DETACH`/`ATTACH` roundtrip.
-- `formatQuerySingleLine` + `extract(..., 'AS .*')` keeps the assertion stable
-- across the database name, the table UUID and column-list formatting.

-- Case 1: `EXCEPT` group on the right of `UNION ALL`. The incident shape.
-- {1} UNION ALL ({2} EXCEPT {1}) = {1, 2}
-- Without the fix: re-parses as ({1} UNION ALL {2}) EXCEPT {1} = {2}.
DROP TABLE IF EXISTS v_union_except_right;

CREATE VIEW v_union_except_right AS SELECT 1 AS x UNION ALL (SELECT 2 EXCEPT SELECT 1);

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_except_right';
SELECT * FROM v_union_except_right ORDER BY all;
SELECT '---';

DETACH TABLE v_union_except_right SYNC;
ATTACH TABLE v_union_except_right;

SELECT * FROM v_union_except_right ORDER BY all;

DROP TABLE v_union_except_right;
SELECT '===';

-- Case 2: `INTERSECT` group on the right of `UNION ALL`.
-- The rows coincide with the mis-parenthesized form, so only the SQL pin
-- catches this one.
DROP TABLE IF EXISTS v_union_intersect_right;

CREATE VIEW v_union_intersect_right AS SELECT 1 AS x UNION ALL (SELECT 2 INTERSECT SELECT 2);

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_intersect_right';
SELECT * FROM v_union_intersect_right ORDER BY all;
SELECT '---';

DETACH TABLE v_union_intersect_right SYNC;
ATTACH TABLE v_union_intersect_right;

SELECT * FROM v_union_intersect_right ORDER BY all;

DROP TABLE v_union_intersect_right;
SELECT '===';

-- Case 3: `EXCEPT DISTINCT` (explicit mode) on the right.
DROP TABLE IF EXISTS v_union_except_distinct;

CREATE VIEW v_union_except_distinct AS SELECT 1 AS x UNION ALL (SELECT 2 EXCEPT DISTINCT SELECT 1);

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_except_distinct';
SELECT * FROM v_union_except_distinct ORDER BY all;
SELECT '---';

DETACH TABLE v_union_except_distinct SYNC;
ATTACH TABLE v_union_except_distinct;

SELECT * FROM v_union_except_distinct ORDER BY all;

DROP TABLE v_union_except_distinct;
SELECT '===';

-- Case 4: `INTERSECT DISTINCT` (explicit mode) on the right.
DROP TABLE IF EXISTS v_union_intersect_distinct;

CREATE VIEW v_union_intersect_distinct AS SELECT 1 AS x UNION ALL (SELECT 2 INTERSECT DISTINCT SELECT 2);

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_intersect_distinct';
SELECT * FROM v_union_intersect_distinct ORDER BY all;
SELECT '---';

DETACH TABLE v_union_intersect_distinct SYNC;
ATTACH TABLE v_union_intersect_distinct;

SELECT * FROM v_union_intersect_distinct ORDER BY all;

DROP TABLE v_union_intersect_distinct;
SELECT '===';

-- Case 5: `EXCEPT` group on the left of `UNION ALL`.
DROP TABLE IF EXISTS v_except_union_left;

CREATE VIEW v_except_union_left AS (SELECT 2 AS x EXCEPT SELECT 1) UNION ALL SELECT 1;

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_except_union_left';
SELECT * FROM v_except_union_left ORDER BY all;
SELECT '---';

DETACH TABLE v_except_union_left SYNC;
ATTACH TABLE v_except_union_left;

SELECT * FROM v_except_union_left ORDER BY all;

DROP TABLE v_except_union_left;
SELECT '===';

-- Case 6: nested `INTERSECT` inside the `EXCEPT` group, itself inside the UNION
-- chain. Exercises this fix and the symmetric one in
-- `ASTSelectIntersectExceptQuery::formatImpl` at the same time.
DROP TABLE IF EXISTS v_union_nested_group;

CREATE VIEW v_union_nested_group AS SELECT 1 AS x UNION ALL (SELECT 3 EXCEPT (SELECT 4 INTERSECT SELECT 5));

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_nested_group';
SELECT * FROM v_union_nested_group ORDER BY all;
SELECT '---';

DETACH TABLE v_union_nested_group SYNC;
ATTACH TABLE v_union_nested_group;

SELECT * FROM v_union_nested_group ORDER BY all;

DROP TABLE v_union_nested_group;
SELECT '===';

-- Case 7: the group in the middle of a three-branch chain, so the child has a
-- sibling on both sides.
-- {1} UNION ALL ({2} EXCEPT {1}) UNION ALL {3} = {1, 2, 3}
-- Without the fix: (({1} UNION ALL {2}) EXCEPT {1}) UNION ALL {3} = {2, 3}. The
-- trailing UNION is folded in after the EXCEPT node is built, so the group binds
-- to its left sibling only.
DROP TABLE IF EXISTS v_union_group_middle;

CREATE VIEW v_union_group_middle AS SELECT 1 AS x UNION ALL (SELECT 2 EXCEPT SELECT 1) UNION ALL SELECT 3;

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_union_group_middle';
SELECT * FROM v_union_group_middle ORDER BY all;
SELECT '---';

DETACH TABLE v_union_group_middle SYNC;
ATTACH TABLE v_union_group_middle;

SELECT * FROM v_union_group_middle ORDER BY all;

DROP TABLE v_union_group_middle;
SELECT '===';

-- Case 8: a standalone `EXCEPT`/`INTERSECT` is also wrapped in a one-child
-- `ASTSelectWithUnionQuery` (`NormalizeSelectWithUnionQueryMatcher` only lifts
-- up `ASTSelectWithUnionQuery` children). It has no sibling to rebind to, so it
-- must NOT gain parentheses -- this pins the `children.size() > 1` guard.
DROP TABLE IF EXISTS v_standalone_except;

CREATE VIEW v_standalone_except AS SELECT 2 AS x EXCEPT SELECT 1;

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_standalone_except';
SELECT * FROM v_standalone_except ORDER BY all;

DROP TABLE v_standalone_except;
SELECT '===';

DROP TABLE IF EXISTS v_standalone_intersect;

CREATE VIEW v_standalone_intersect AS SELECT 2 AS x INTERSECT SELECT 2;

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'v_standalone_intersect';
SELECT * FROM v_standalone_intersect ORDER BY all;

DROP TABLE v_standalone_intersect;
SELECT '===';

-- Case 9: the reported failure. A materialized view whose stored definition was
-- rewritten into a shape `checkAllowedQueries` rejects, which made the metadata
-- unloadable and stopped the server from starting.
-- The first UNION branch must stay FROM-less: `checkAllowedQueries` returns
-- early on a branch with no table expression, which is why this definition is
-- accepted at DDL time in the first place. That acceptance is unchanged here;
-- what changes is that the persisted text now matches what was submitted.
DROP TABLE IF EXISTS mv_src;
DROP TABLE IF EXISTS mv_union_except;

CREATE TABLE mv_src (x UInt64) ENGINE = MergeTree ORDER BY x;

CREATE MATERIALIZED VIEW mv_union_except (x UInt64) ENGINE = MergeTree ORDER BY x
AS SELECT 0 AS x UNION ALL (SELECT x FROM mv_src EXCEPT SELECT 1);

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'mv_union_except';

DROP TABLE mv_union_except;
SELECT '===';

-- Case 10: a plain multi-branch `UNION ALL` materialized view keeps working and
-- keeps receiving inserts, so the added parentheses cannot break the ordinary
-- case.
DROP TABLE IF EXISTS mv_plain_union;

CREATE MATERIALIZED VIEW mv_plain_union (x UInt64) ENGINE = MergeTree ORDER BY x
AS SELECT x FROM mv_src UNION ALL SELECT x + 100 FROM mv_src;

SELECT replaceAll(extract(formatQuerySingleLine(create_table_query), 'AS .*'), currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 'mv_plain_union';

DETACH TABLE mv_plain_union SYNC;
ATTACH TABLE mv_plain_union;

INSERT INTO mv_src VALUES (1);
SELECT * FROM mv_plain_union ORDER BY all;

DROP TABLE mv_plain_union;
DROP TABLE mv_src;
