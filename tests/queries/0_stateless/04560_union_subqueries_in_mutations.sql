-- Set operators (UNION / UNION ALL / UNION DISTINCT / EXCEPT / INTERSECT) inside a subquery must work
-- in the predicate and in the assignment expressions of DELETE and ALTER UPDATE mutations.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/72853

DROP TABLE IF EXISTS t_union_mut;
CREATE TABLE t_union_mut (c0 Int) ENGINE = MergeTree ORDER BY tuple();

SET mutations_sync = 1;

-- DELETE with UNION DISTINCT in the predicate.
INSERT INTO t_union_mut VALUES (0), (1), (2);
DELETE FROM t_union_mut WHERE c0 IN ((SELECT 1) UNION DISTINCT (SELECT 0));
SELECT 'delete union distinct', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- DELETE with UNION ALL in the predicate.
INSERT INTO t_union_mut VALUES (0), (1), (2);
DELETE FROM t_union_mut WHERE c0 IN ((SELECT 1) UNION ALL (SELECT 0));
SELECT 'delete union all', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- DELETE with EXCEPT in the predicate.
INSERT INTO t_union_mut VALUES (0), (1), (2);
DELETE FROM t_union_mut WHERE c0 IN ((SELECT 1) EXCEPT (SELECT 0));
SELECT 'delete except', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- DELETE with INTERSECT in the predicate.
INSERT INTO t_union_mut VALUES (0), (1), (2);
DELETE FROM t_union_mut WHERE c0 IN ((SELECT 1) INTERSECT (SELECT 1));
SELECT 'delete intersect', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- ALTER UPDATE with UNION in the predicate.
INSERT INTO t_union_mut VALUES (0), (1), (2);
ALTER TABLE t_union_mut UPDATE c0 = 9 WHERE c0 IN ((SELECT 1) UNION DISTINCT (SELECT 0));
SELECT 'update union predicate', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- ALTER UPDATE with UNION in the assignment expression.
INSERT INTO t_union_mut VALUES (0), (1), (2);
ALTER TABLE t_union_mut UPDATE c0 = (SELECT sum(x) FROM ((SELECT 1 AS x) UNION DISTINCT (SELECT 2 AS x))) WHERE c0 = 1;
SELECT 'update union assignment', arraySort(groupArray(c0)) FROM t_union_mut;
TRUNCATE TABLE t_union_mut;

-- The same must also work with the old analyzer.
SET enable_analyzer = 0;
INSERT INTO t_union_mut VALUES (0), (1), (2);
DELETE FROM t_union_mut WHERE c0 IN ((SELECT 1) UNION DISTINCT (SELECT 0));
SELECT 'delete union distinct, old analyzer', arraySort(groupArray(c0)) FROM t_union_mut;

DROP TABLE t_union_mut;

SET enable_analyzer = 1;

-- A lightweight UPDATE (the default `update_parallel_mode = 'auto'`) computes its affected columns via
-- `getUpdateAffectedColumns`, which builds the query tree for the re-parsed predicate and assignment
-- expressions. Set operators in those subqueries must be normalized there too, otherwise the lightweight
-- update fails before the patch-part pipeline is built.
DROP TABLE IF EXISTS t_union_lwu;
CREATE TABLE t_union_lwu (c0 Int, c1 Int) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SET enable_lightweight_update = 1;

INSERT INTO t_union_lwu VALUES (0, 0), (1, 1), (2, 2);
UPDATE t_union_lwu SET c0 = 9 WHERE c1 IN ((SELECT 1) UNION DISTINCT (SELECT 0));
SELECT 'lightweight update union predicate', arraySort(groupArray(c0)) FROM t_union_lwu;

TRUNCATE TABLE t_union_lwu;
INSERT INTO t_union_lwu VALUES (0, 0), (1, 1), (2, 2);
UPDATE t_union_lwu SET c0 = (SELECT sum(x) FROM ((SELECT 1 AS x) UNION DISTINCT (SELECT 2 AS x))) WHERE c1 = 1;
SELECT 'lightweight update union assignment', arraySort(groupArray(c0)) FROM t_union_lwu;

DROP TABLE t_union_lwu;

-- An unfinished mutation may now legitimately contain a set operation in a subquery. A concurrent
-- DROP COLUMN / RENAME COLUMN must still be able to inspect that mutation's column dependencies without
-- failing to normalize the subquery, as long as it does not touch a column the mutation references.
-- (`checkDropOrRenameCommandDoesntAffectInProgressMutations` handles DROP and RENAME with the same code.)
DROP TABLE IF EXISTS t_union_inprogress;
CREATE TABLE t_union_inprogress (c0 Int, c1 Int, c2 Int) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_union_inprogress VALUES (0, 0, 0), (1, 1, 1), (2, 2, 2);

-- Stop merges so the mutations below stay in progress while we ALTER.
SYSTEM STOP MERGES t_union_inprogress;
SET mutations_sync = 0;

-- One in-progress mutation with a set operation in the predicate, one with it in the assignment.
ALTER TABLE t_union_inprogress UPDATE c0 = 5 WHERE c1 IN ((SELECT 1) UNION DISTINCT (SELECT 0));
ALTER TABLE t_union_inprogress UPDATE c1 = (SELECT sum(x) FROM ((SELECT 1 AS x) UNION DISTINCT (SELECT 2 AS x))) WHERE c0 = 1;

-- Dropping an unrelated column must inspect the in-progress mutations' predicate and assignment
-- subqueries and succeed rather than fail to normalize them.
ALTER TABLE t_union_inprogress DROP COLUMN c2 SETTINGS alter_sync = 0;
SELECT 'drop column with in-progress union mutation', 'ok';

SYSTEM START MERGES t_union_inprogress;
DROP TABLE t_union_inprogress;
