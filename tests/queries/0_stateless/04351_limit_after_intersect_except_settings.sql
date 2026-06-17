-- The `limit`/`offset` settings cap must be applied once over the final set-operation result, not once
-- per branch. INTERSECT/EXCEPT also plan through UnionNode (analyzer), so range branches must move their
-- settings cap onto the set-operation node just like UNION ALL.
-- Note: the legacy interpreter (enable_analyzer = 0) does not apply the `limit` setting to set
-- operations at all (pre-existing behavior, unrelated to LIMIT AFTER), so this covers the analyzer path.

-- { echo }

-- INTERSECT of 0..9 with itself = 0..9; settings limit caps to 3.
(SELECT number AS n FROM numbers(10) LIMIT AFTER number >= 0) INTERSECT (SELECT number AS n FROM numbers(10) LIMIT AFTER number >= 0) SETTINGS limit = 3;

-- EXCEPT: 0..9 except 0..4 = 5..9; settings limit caps to 2.
(SELECT number AS n FROM numbers(10) LIMIT AFTER number >= 0) EXCEPT (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2;

-- UNTIL branch variant: left branch 0..7 (until 8), intersect with 0..9 = 0..7, cap to 2.
(SELECT number AS n FROM numbers(10) LIMIT UNTIL number >= 8) INTERSECT (SELECT number AS n FROM numbers(10) LIMIT AFTER number >= 0) SETTINGS limit = 2;

-- Branch-local settings must remain scoped to their own subqueries.
SELECT * FROM (SELECT * FROM (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0 SETTINGS limit = 2, offset = 1) UNION ALL SELECT * FROM (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0 SETTINGS limit = 2, offset = 2)) ORDER BY n;

-- Mixed range/non-range branches must apply the settings cap once over the final set-operation result.
(SELECT number + 10 AS n FROM numbers(5)) UNION ALL (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 3, offset = 4;

-- Query-tree-to-AST conversion must preserve the settings cap carried by UnionNode.
EXPLAIN SYNTAX (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0) UNION ALL (SELECT number AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2, offset = 1;
