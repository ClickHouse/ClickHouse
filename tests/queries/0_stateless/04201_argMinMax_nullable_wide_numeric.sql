-- Test: exercises `getGreatestIndexNotNullIf`/`getSmallestIndexNotNullIf` slow path
-- with `Nullable(Decimal128)`, `Nullable(Decimal256)`, `Nullable(Int128)`, `Nullable(UInt256)`.
-- Covers: src/AggregateFunctions/SingleValueData.cpp:776-778 — the `vec_data[i] > vec_data[index]`
-- branch is reached only for types NOT in `has_find_extreme_implementation` /
-- `underlying_has_find_extreme_implementation` (i.e., wide ints and Decimal128/256).
-- The PR's own test uses `Nullable(DateTime64)` which since then was added to the fast path,
-- so the slow-path comparison branch (the bug-fix line) has zero coverage in the suite.

DROP TABLE IF EXISTS t_argminmax_wide;

CREATE TABLE t_argminmax_wide
(
    v Int64,
    t_dec128 Nullable(Decimal128(0)),
    t_dec256 Nullable(Decimal256(0)),
    t_int128 Nullable(Int128),
    t_uint256 Nullable(UInt256)
) ENGINE = MergeTree ORDER BY tuple();

-- Greatest key=30 at v=3 (middle row), smallest key=10 at v=1 (first row).
-- A buggy `<`-instead-of-`>` slow path would never update the index past row 0
-- and return v=1 for both argMax and argMin.
INSERT INTO t_argminmax_wide VALUES
    (1, 10, 10,  10,  10),
    (2, 20, 20,  20,  20),
    (3, 30, 30,  30,  30),
    (4, 25, 25,  25,  25),
    (5, 15, 15,  15,  15);

SELECT 'Decimal128',
    argMax(v, t_dec128),
    argMin(v, t_dec128),
    argMaxIf(v, t_dec128, v != 3),
    argMinIf(v, t_dec128, v != 1)
FROM t_argminmax_wide;

SELECT 'Decimal256',
    argMax(v, t_dec256),
    argMin(v, t_dec256),
    argMaxIf(v, t_dec256, v != 3),
    argMinIf(v, t_dec256, v != 1)
FROM t_argminmax_wide;

SELECT 'Int128',
    argMax(v, t_int128),
    argMin(v, t_int128),
    argMaxIf(v, t_int128, v != 3),
    argMinIf(v, t_int128, v != 1)
FROM t_argminmax_wide;

SELECT 'UInt256',
    argMax(v, t_uint256),
    argMin(v, t_uint256),
    argMaxIf(v, t_uint256, v != 3),
    argMinIf(v, t_uint256, v != 1)
FROM t_argminmax_wide;

DROP TABLE t_argminmax_wide;
