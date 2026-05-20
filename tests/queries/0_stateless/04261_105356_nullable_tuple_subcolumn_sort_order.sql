-- Tags: no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105356
--
-- Background: ORDER BY a `Nullable` subcolumn of a `Nullable(Tuple(..., Nullable(X), ...))`.
-- `SummingMergeTree` rebuilds the sort key during merge via `getSubcolumn(c0, '3')`, which
-- goes through `NullableSubcolumnCreator::create`. When the inner subcolumn is itself a
-- `Nullable` column the creator used to return it unchanged, dropping the outer null map.
-- Rows that were written with `outer = NULL` and `inner` not flagged as `NULL` (which
-- `generateRandom` produces) then exposed their inner value, contradicting the on-disk
-- sort key that combined both masks. The next horizontal merge tripped
-- `CheckSortedTransform`:
--   `Logical error: 'Sort order of blocks violated for column number 2,
--    left: NULL, right: Decimal64_'<value>'. Chunk 0, rows read N.'`
--
-- The fix `OR`s the outer null map into the inner null map. This test exercises that path
-- by inserting several parts of `generateRandom` data and forcing a horizontal merge.

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_105356;

CREATE TABLE t_105356
(
    c0 Nullable(Tuple(String, Nullable(String), Nullable(Decimal))),
    c1 String,
    c2 Decimal,
    c3 String
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.3`, c2, c3)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 101, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 201, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 301, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 401, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 501, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 601, 10, 0) LIMIT 10;
INSERT INTO t_105356 (c2, c3, c1, c0) SELECT c2, c3, c1, c0 FROM generateRandom('`c2` Decimal, `c3` String, `c1` String, `c0` Nullable(Tuple(String, Nullable(String), Nullable(Decimal)))', 701, 10, 0) LIMIT 10;

-- This `OPTIMIZE FINAL` is the action that used to abort the server with the
-- `Sort order of blocks violated` `LOGICAL_ERROR`. With the fix the merge succeeds and
-- collapses some rows by `SummingMergeTree` semantics.
OPTIMIZE TABLE t_105356 FINAL;

-- Assert the merge produced a single part and the row count is non-zero. The exact count
-- depends on `SummingMergeTree` collapse, but it must be deterministic across runs
-- because the seeds above are fixed.
SELECT count() > 0, uniqExact(_part) FROM t_105356;

-- Verify the outer null map is respected when reading `c0.3` directly: rows where the
-- outer tuple is `NULL` must report the subcolumn as `NULL` too, regardless of what
-- `generateRandom` placed into the inner `Nullable` column.
SELECT countIf(c0 IS NULL AND isNotNull(`c0.3`)) FROM t_105356;

DROP TABLE t_105356;
