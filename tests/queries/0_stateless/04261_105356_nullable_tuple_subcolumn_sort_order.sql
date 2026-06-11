-- Tags: no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/105356
--
-- A row written as `outer tuple = NULL` can still carry a non-NULL inner element on disk.
-- Extracting that element as a sort-key subcolumn must re-apply the outer null map; the old
-- code dropped it, so the merged sort key disagreed with the on-disk order and the next
-- horizontal merge tripped `CheckSortedTransform` with
-- `Logical error: 'Sort order of blocks violated ...'`.
--
-- `if(cond, NULL, tuple(...))` deterministically produces that poison on-disk state (outer
-- NULL, inner stored non-NULL), so the repro does not depend on `generateRandom` data, which
-- varies by architecture.

SET allow_experimental_nullable_tuple_type = 1;

-- Carrier 1: inner element is `Nullable(Decimal)`, extracted as a `ColumnNullable`.
DROP TABLE IF EXISTS t_105356;

CREATE TABLE t_105356
(
    c0 Nullable(Tuple(String, Nullable(Decimal))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_105356 SELECT if(number = 0, NULL, ('a', toDecimal32(10, 0))), toDecimal32(number, 0) FROM numbers(2);
INSERT INTO t_105356 SELECT if(number = 0, NULL, ('b', toDecimal32(20, 0))), toDecimal32(number + 5, 0) FROM numbers(2);

-- This `OPTIMIZE FINAL` used to abort the server with `Sort order of blocks violated`.
OPTIMIZE TABLE t_105356 FINAL;

SELECT count() > 0, uniqExact(_part) FROM t_105356;
-- Outer null map respected when reading `c0.2`: an outer-NULL row reports the subcolumn NULL.
SELECT countIf(c0 IS NULL AND isNotNull(`c0.2`)) FROM t_105356;

DROP TABLE t_105356;

-- Carrier 2: inner element is `LowCardinality(Nullable(String))`. It carries nullability in its
-- dictionary, so it reaches the creator as a plain `ColumnLowCardinality` (not a `ColumnNullable`).
DROP TABLE IF EXISTS t_105356_lc;

CREATE TABLE t_105356_lc
(
    c0 Nullable(Tuple(String, LowCardinality(Nullable(String)))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1;

INSERT INTO t_105356_lc SELECT if(number = 0, NULL, ('a', toLowCardinality(CAST('p', 'Nullable(String)')))), toDecimal32(number, 0) FROM numbers(2);
INSERT INTO t_105356_lc SELECT if(number = 0, NULL, ('b', toLowCardinality(CAST('q', 'Nullable(String)')))), toDecimal32(number + 5, 0) FROM numbers(2);

OPTIMIZE TABLE t_105356_lc FINAL;

SELECT count() > 0, uniqExact(_part) FROM t_105356_lc;
SELECT countIf(c0 IS NULL AND isNotNull(`c0.2`)) FROM t_105356_lc;

DROP TABLE t_105356_lc;

-- Carrier 3: inner element is a NON-nullable `LowCardinality(String)`. It cannot carry NULLs by
-- itself, so the creator must promote it to `LowCardinality(Nullable(String))` before folding in
-- the outer mask. `index_granularity = 1` puts each row in its own granule so the merge compares
-- the leaked inner value across granule boundaries.
DROP TABLE IF EXISTS t_105356_lc_nn;

CREATE TABLE t_105356_lc_nn
(
    c0 Nullable(Tuple(String, LowCardinality(String))),
    c2 Decimal
)
ENGINE = SummingMergeTree()
ORDER BY (`c0.2`, c2)
SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO t_105356_lc_nn SELECT if(number % 2 = 0, NULL, ('a', toLowCardinality('p'))), toDecimal32(number, 0) FROM numbers(4);
INSERT INTO t_105356_lc_nn SELECT if(number % 2 = 0, NULL, ('b', toLowCardinality('q'))), toDecimal32(number + 10, 0) FROM numbers(4);

OPTIMIZE TABLE t_105356_lc_nn FINAL;

-- The subcolumn type here is non-nullable `LowCardinality(String)`, so `isNotNull(c0.2)` is always
-- true; the meaningful assertion is simply that the merge above completed (single part, rows kept).
SELECT count() > 0, uniqExact(_part) FROM t_105356_lc_nn;

DROP TABLE t_105356_lc_nn;
