-- Tags: no-parallel
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

DROP VIEW test_view_pruning;
DROP VIEW test_view_system;
DROP TABLE test_subcolumn_pruning;
