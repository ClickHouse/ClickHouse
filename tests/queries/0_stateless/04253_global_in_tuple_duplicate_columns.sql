-- Tags: no-replicated-database, no-parallel-replicas
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103228
--
-- `GLOBAL IN` with a tuple LHS and a subquery RHS whose projection produces
-- columns with duplicate names (e.g. `SELECT number, number FROM ...`) used
-- to fail with `ILLEGAL_COLUMN: Cannot add column number: column with this
-- name already exists` when the filter pushdown path
-- (`allow_push_predicate_ast_for_distributed_subqueries = 1`) was active.
--
-- `tryBuildAdditionalFilterAST` in `ReadFromRemote.cpp` built a temporary
-- table for the GLOBAL IN set from the subquery sample block without
-- deduplicating column names, while `ColumnsDescription` prohibits duplicate
-- names. The sibling path in `buildQueryTreeForShard.cpp` already calls
-- `makeUniqueColumnNamesInBlock` for the same reason — that pattern is now
-- applied here too.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET prefer_localhost_replica = 0;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;

DROP TABLE IF EXISTS t_gin_dup;

CREATE TABLE t_gin_dup (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_gin_dup SELECT number, number FROM numbers(100);

-- Baseline: single-column GLOBAL IN still works.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE x GLOBAL IN (SELECT number FROM numbers(5));

-- Baseline: tuple GLOBAL IN where the subquery projects distinct names still works.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, y) GLOBAL IN (SELECT toUInt32(number) AS a, toUInt32(number) AS b FROM numbers(5));

-- Regression: tuple GLOBAL IN with duplicate column names in the subquery
-- projection. This used to throw ILLEGAL_COLUMN.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, y) GLOBAL IN (SELECT number, number FROM numbers(5));

-- More than two duplicates in the tuple.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, x, y) GLOBAL IN (SELECT number, number, number FROM numbers(5));

-- Suffix collision: the deduplication step inside `makeUniqueColumnNamesInBlock`
-- renames the second `a` to `a_1`, but the projection also produces a column
-- named `a_1`. The helper must keep looping until it finds a truly unused
-- suffix (`a_2` here) and must register every renamed name, otherwise the
-- resulting block still has duplicate column names and `ColumnsDescription`
-- throws `ILLEGAL_COLUMN`.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, x, y) GLOBAL IN (
    SELECT toUInt32(number) AS a, toUInt32(number) AS a, toUInt32(number) AS a_1 FROM numbers(5)
);

-- Larger suffix-collision shape: the projection already contains `a_1` and
-- `a_2`, so collision-safe renaming must skip them and pick `a_3`/`a_4`.
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, x, x, y, y) GLOBAL IN (
    SELECT
        toUInt32(number) AS a,
        toUInt32(number) AS a,
        toUInt32(number) AS a,
        toUInt32(number) AS a_1,
        toUInt32(number) AS a_2
    FROM numbers(5)
);

-- Correctness: nothing in the set, result must be 0 (not a spurious match
-- caused by silently dropping the predicate).
SELECT sum(y) FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_gin_dup))
WHERE (x, y) GLOBAL IN (SELECT 1000 + number, 1000 + number FROM numbers(5));

DROP TABLE t_gin_dup;
