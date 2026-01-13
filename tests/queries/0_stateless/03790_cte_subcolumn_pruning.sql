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

DROP TABLE test_subcolumn_pruning;
