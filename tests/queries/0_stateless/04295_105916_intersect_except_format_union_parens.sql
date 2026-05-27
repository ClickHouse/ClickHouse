-- https://github.com/ClickHouse/ClickHouse/issues/105916
-- `ASTSelectIntersectExceptQuery::formatImpl` must wrap `ASTSelectWithUnionQuery` children
-- in parentheses, otherwise the lower-precedence `UNION` on the right (or left) side
-- gets reparsed as part of the outer query and the view returns a wrong result after
-- `DETACH`/`ATTACH`.

-- Case 1: `EXCEPT` with `UNION ALL` on the right.
-- {2} EXCEPT {2, 1} = {} (empty)
-- Without the fix: re-parses as ({2} EXCEPT {2}) UNION ALL {1} = {1}.
DROP TABLE IF EXISTS v_except_union_right;

CREATE VIEW v_except_union_right AS SELECT 2 c0 EXCEPT (SELECT 2 UNION ALL SELECT 1);

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

SELECT * FROM v_except_union_distinct ORDER BY all;
SELECT '---';

DETACH TABLE v_except_union_distinct SYNC;
ATTACH TABLE v_except_union_distinct;

SELECT * FROM v_except_union_distinct ORDER BY all;

DROP TABLE v_except_union_distinct;
