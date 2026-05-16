-- Regression test for STID 3520-3e11: 'Bad cast from type DB::ColumnVector to DB::ColumnNullable'.
--
-- A `MATERIALIZED` CTE body that references an identifier resolved from outer scope
-- (e.g. a projection alias of the calling subquery) was inlined as different constants
-- in different references, producing different inferred projection types per reference.
-- The single shared storage was created from the first reference's types, while the
-- materializer pipeline could end up using a later reference's types — the resulting
-- block was written into the storage with mismatched column types and any reader that
-- relied on the storage metadata then crashed in `MemorySource::fillPhysicalColumns` →
-- `SerializationNullable::enumerateStreams` while trying to `assert_cast` a non-Nullable
-- column to `ColumnNullable`.
--
-- See https://github.com/ClickHouse/ClickHouse/issues/103813.
--
-- The query analyzer now detects this drift across references and rejects the query
-- with `TYPE_MISMATCH` (error code 53) instead of crashing the server with a
-- `LOGICAL_ERROR`. Verify that all known reproducer shapes are rejected cleanly.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- Original reproducer from issue #103813. The CTE body references `a` which is not
-- defined inside; each UNION branch supplies a different constant for `a`, so the
-- two clones of the materialized CTE infer `x` as `Nullable(Nothing)` and `UInt64`
-- respectively.
SELECT * FROM (
    WITH t AS MATERIALIZED (SELECT a + number AS x FROM numbers(65536))
    SELECT * FROM (
        SELECT NULL AS a, x FROM t LIMIT 791
        UNION ALL
        SELECT 2147483647 AS a, x FROM t
    ) ORDER BY a DESC, x ASC LIMIT 942
) FORMAT Null; -- { serverError TYPE_MISMATCH }

-- Same shape but with `DISTINCT` and the second branch's columns swapped — another
-- variant seen in the wild.
SELECT * FROM (
    WITH t AS MATERIALIZED (SELECT DISTINCT number + a AS x FROM numbers(256))
    SELECT * FROM (
        SELECT DISTINCT NULL AS a, x FROM t
        UNION ALL
        SELECT x, 100 AS a FROM t
    ) ORDER BY a ASC NULLS FIRST, x ASC
) FORMAT Null; -- { serverError TYPE_MISMATCH }

-- Variant that lands on `Float64` (`number / a`) instead of `UInt64` — confirms the
-- check is not specialised to one type pair.
SELECT * FROM (
    WITH t AS MATERIALIZED (SELECT DISTINCT number / a AS x FROM numbers(65536) LIMIT 1048575)
    SELECT * FROM (
        SELECT NULL AS a, x FROM t
        UNION ALL
        SELECT x, 1024 AS a FROM t
    ) ORDER BY a DESC, x ASC NULLS FIRST WITH FILL
) FORMAT Null; -- { serverError TYPE_MISMATCH }

-- Sanity checks: shapes that previously worked must still work after the fix.

-- The CTE body has no outer-scope dependency, so all references agree on the type.
WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(3))
SELECT a, x FROM (
    SELECT 1 AS a, x FROM t
    UNION ALL
    SELECT 2 AS a, x FROM t
) ORDER BY a, x;

-- Single-reference materialized CTE.
WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(3))
SELECT x FROM t ORDER BY x;

-- Materialized CTE referenced multiple times via JOIN within the same scope.
WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(3))
SELECT t1.x AS x1, t2.x AS x2
FROM t AS t1 JOIN t AS t2 ON t1.x = t2.x
ORDER BY x1;
