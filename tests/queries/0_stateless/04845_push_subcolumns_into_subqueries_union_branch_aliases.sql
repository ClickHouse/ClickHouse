-- Tags: no-parallel-replicas, no-random-settings
-- The test checks EXPLAIN output, which differs with parallel replicas and randomized plan-related settings.

-- The whole-column guard and the dead-export analysis must take the aliasing structure of
-- every branch of a UNION ALL into account, not only the leftmost leaf query: the pushdown is
-- applied to all branches or to none, so a single branch exporting the same physical column
-- under another name that stays alive is enough for that branch to read both the whole column
-- and the subcolumn.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_push_branch_aliases;
DROP TABLE IF EXISTS t_push_branch_aliases_2;

CREATE TABLE t_push_branch_aliases (id UInt32, tup Tuple(a UInt32, b String), other Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

CREATE TABLE t_push_branch_aliases_2 (id UInt32, tup Tuple(a UInt32, b String))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_push_branch_aliases VALUES (1, (1, 'one'), (10, 'ten'));
INSERT INTO t_push_branch_aliases_2 VALUES (2, (2, 'two'));

SELECT 'aliasing only in the non-leftmost branch is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, other FROM (SELECT tup AS x, other FROM t_push_branch_aliases UNION ALL SELECT tup AS x, tup AS other FROM t_push_branch_aliases_2)) WHERE explain LIKE '%Output%';
SELECT x.a, other FROM (SELECT tup AS x, other FROM t_push_branch_aliases UNION ALL SELECT tup AS x, tup AS other FROM t_push_branch_aliases_2) ORDER BY ALL;

SELECT 'aliasing only in the leftmost branch is not rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, other FROM (SELECT tup AS x, tup AS other FROM t_push_branch_aliases_2 UNION ALL SELECT tup AS x, other FROM t_push_branch_aliases)) WHERE explain LIKE '%Output%';
SELECT x.a, other FROM (SELECT tup AS x, tup AS other FROM t_push_branch_aliases_2 UNION ALL SELECT tup AS x, other FROM t_push_branch_aliases) ORDER BY ALL;

SELECT 'no cross-branch aliasing is rewritten';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a, other FROM (SELECT tup AS x, other FROM t_push_branch_aliases UNION ALL SELECT tup AS x, other FROM t_push_branch_aliases)) WHERE explain LIKE '%Output%';
SELECT x.a, other FROM (SELECT tup AS x, other FROM t_push_branch_aliases UNION ALL SELECT tup AS x, other FROM t_push_branch_aliases) ORDER BY ALL;

SELECT 'dead alias-equivalent sibling through a non-leftmost branch class';
-- The export `y` is never referenced and is alias-equivalent to the dead export `x` only in
-- the second branch; the class of the second branch must mark `y` dead wholesale, so that the
-- whole-column reference inside the `y` slot of the second branch does not block the deeper
-- pushdown into the nested subqueries.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x.a FROM (SELECT tup AS x, other AS y FROM (SELECT tup, other FROM t_push_branch_aliases) UNION ALL SELECT tup AS x, tup AS y FROM (SELECT tup FROM t_push_branch_aliases_2))) WHERE explain LIKE '%Output%';
SELECT x.a FROM (SELECT tup AS x, other AS y FROM (SELECT tup, other FROM t_push_branch_aliases) UNION ALL SELECT tup AS x, tup AS y FROM (SELECT tup FROM t_push_branch_aliases_2)) ORDER BY ALL;

DROP TABLE t_push_branch_aliases;
DROP TABLE t_push_branch_aliases_2;
