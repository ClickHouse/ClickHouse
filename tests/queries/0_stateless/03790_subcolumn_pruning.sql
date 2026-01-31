-- Requires new analyzer - SubcolumnPushdownPass is part of the new analyzer infrastructure
SET enable_analyzer = 1;

-- Test subcolumn pruning works through CTEs and subqueries
-- Verifies only the needed subcolumn (tup.a) is read, not the entire Tuple

DROP TABLE IF EXISTS test_subcolumn_pruning;

CREATE TABLE test_subcolumn_pruning (
    id UInt64,
    tup Tuple(a String, b Int32)
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO test_subcolumn_pruning VALUES (1, ('x', 1));

-- Direct query should read only tup.a
SELECT 'direct';
SELECT count() > 0 FROM (EXPLAIN header=1 SELECT tup.a FROM test_subcolumn_pruning)
    WHERE explain LIKE '%tup.a String%';

-- CTE with SELECT * should also read only tup.a (not full tup Tuple)
SELECT 'cte';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    WITH foo AS (SELECT * FROM test_subcolumn_pruning)
    SELECT tup.a FROM foo
) WHERE explain LIKE '%tup.a String%';

-- Subquery with SELECT * should also read only tup.a
SELECT 'subquery';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    SELECT tup.a FROM (SELECT * FROM test_subcolumn_pruning)
) WHERE explain LIKE '%tup.a String%';

-- VIEW with SELECT * should also read only tup.a (table supports optimization)
DROP VIEW IF EXISTS test_view_pruning;
CREATE VIEW test_view_pruning AS SELECT * FROM test_subcolumn_pruning;
SELECT 'view';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    SELECT tup.a FROM test_view_pruning
) WHERE explain LIKE '%tup.a String%';

-- VIEW over system table (doesn't support subcolumn optimization) should still work
DROP VIEW IF EXISTS test_view_system;
CREATE VIEW test_view_system AS SELECT * FROM system.one;
SELECT 'view_system_table';
SELECT dummy IS NULL FROM test_view_system; -- Should work without error (dummy is not nullable but query should execute)

-- VIEW with multiple columns including subcolumn access
SELECT 'view_multi_column_with_subcolumn';
SELECT id, tup.a FROM test_view_pruning;

-- VIEW + JOIN USING with subcolumn access
DROP TABLE IF EXISTS test_join_target;
CREATE TABLE test_join_target (id UInt64, test String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_join_target VALUES (1, 'test1');

DROP VIEW IF EXISTS test_view_join_target;
CREATE VIEW test_view_join_target AS SELECT * FROM test_join_target;

SELECT 'view_join_using_subcolumn';
SELECT test, id, tup.a
FROM test_view_pruning
JOIN test_view_join_target USING id;

DROP VIEW test_view_join_target;
DROP TABLE test_join_target;

-- Verify the full Tuple is NOT being read (would show "tup Tuple")
SELECT 'no_full_tuple_direct';
SELECT count() == 0 FROM (EXPLAIN header=1 SELECT tup.a FROM test_subcolumn_pruning)
    WHERE explain LIKE '%tup Tuple%';

SELECT 'no_full_tuple_cte';
SELECT count() == 0 FROM (
    EXPLAIN header=1
    WITH foo AS (SELECT * FROM test_subcolumn_pruning)
    SELECT tup.a FROM foo
) WHERE explain LIKE '%tup Tuple%';

SELECT 'no_full_tuple_subquery';
SELECT count() == 0 FROM (
    EXPLAIN header=1
    SELECT tup.a FROM (SELECT * FROM test_subcolumn_pruning)
) WHERE explain LIKE '%tup Tuple%';

SELECT 'no_full_tuple_view';
SELECT count() == 0 FROM (
    EXPLAIN header=1
    SELECT tup.a FROM test_view_pruning
) WHERE explain LIKE '%tup Tuple%';

-- JOIN with subcolumn access should work without crashing
-- (Regression test: subcolumn pushdown must not clone inside JoinNode
-- as it would invalidate column references in the ON clause)
SELECT 'join_subquery_subcolumn';
SELECT a.tup.a, b.tup.b
FROM (SELECT * FROM test_subcolumn_pruning) a
JOIN (SELECT * FROM test_subcolumn_pruning) b ON a.id = b.id;

-- Also test with Map type which was the original failure case
DROP TABLE IF EXISTS test_map_join;
CREATE TABLE test_map_join (
    id UInt64,
    ProfileEvents Map(LowCardinality(String), UInt64)
) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_map_join VALUES (1, {'event1': 100, 'event2': 200});

SELECT 'join_map_subcolumn';
SELECT a.ProfileEvents.keys, b.ProfileEvents.values
FROM (SELECT * FROM test_map_join) a
JOIN (SELECT * FROM test_map_join) b ON a.id = b.id;

-- Test multiple subcolumns from same base column (tup.a AND tup.b)
-- This was previously causing LOGICAL_ERROR with CTEs/UNIONs

-- CTE: verify both subcolumns are read but NOT the full tuple
SELECT 'multi_subcolumn_cte_has_a';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    WITH sub AS (SELECT * FROM test_subcolumn_pruning)
    SELECT tup.a, tup.b FROM sub
) WHERE explain LIKE '%tup.a String%';

SELECT 'multi_subcolumn_cte_has_b';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    WITH sub AS (SELECT * FROM test_subcolumn_pruning)
    SELECT tup.a, tup.b FROM sub
) WHERE explain LIKE '%tup.b Int32%';

SELECT 'multi_subcolumn_cte_no_full_tuple';
SELECT count() == 0 FROM (
    EXPLAIN header=1
    WITH sub AS (SELECT * FROM test_subcolumn_pruning)
    SELECT tup.a, tup.b FROM sub
) WHERE explain LIKE '%tup Tuple%';

-- Subquery: verify both subcolumns are read but NOT the full tuple
SELECT 'multi_subcolumn_subquery_has_a';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    SELECT tup.a, tup.b FROM (SELECT * FROM test_subcolumn_pruning)
) WHERE explain LIKE '%tup.a String%';

SELECT 'multi_subcolumn_subquery_has_b';
SELECT count() > 0 FROM (
    EXPLAIN header=1
    SELECT tup.a, tup.b FROM (SELECT * FROM test_subcolumn_pruning)
) WHERE explain LIKE '%tup.b Int32%';

SELECT 'multi_subcolumn_subquery_no_full_tuple';
SELECT count() == 0 FROM (
    EXPLAIN header=1
    SELECT tup.a, tup.b FROM (SELECT * FROM test_subcolumn_pruning)
) WHERE explain LIKE '%tup Tuple%';

-- UNION: verify query executes without LOGICAL_ERROR (pushdown through UNION not yet supported)
SELECT 'multi_subcolumn_union_no_error';
WITH sub AS (
    SELECT tup FROM test_subcolumn_pruning
    UNION ALL
    SELECT tup FROM test_subcolumn_pruning
)
SELECT count() FROM (SELECT tup.a, tup.b FROM (SELECT * FROM sub));

DROP TABLE test_map_join;

DROP VIEW test_view_pruning;
DROP VIEW test_view_system;
DROP TABLE test_subcolumn_pruning;
