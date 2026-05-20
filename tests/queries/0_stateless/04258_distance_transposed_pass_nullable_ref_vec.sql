-- Regression test: `DistanceTransposedPartialReadsPass` should not throw a logical error
-- when an argument makes the function result `Nullable` (not only the `Nullable(Nothing)`
-- subcase already covered by `03829_distance_transposed_pass_nullable_nothing.sql`).
--
-- Reproducer: an `if(cond, NULL, [...])` reference vector makes the function return
-- `Nullable(Float64)`. The pass used to cast the reference vector to a non-nullable
-- `Array(...)` and abort with `Before: Nullable(Float64), after: Float64` (STID 0250-3bee).

DROP TABLE IF EXISTS qbit_test;
CREATE TABLE qbit_test (id UInt64, vec QBit(Float32, 3)) ENGINE = Memory;
INSERT INTO qbit_test VALUES (1, [0,1,2]), (2, [1,2,3]), (3, [2,3,4]);

-- `if` makes the second argument's type unify into a nullable variant, so the function
-- returns `Nullable(Float64)`. Before the fix, this triggered the `LOGICAL_ERROR`. Using
-- `id > 0` as the predicate keeps the result non-null on every row, so the output stays
-- deterministic while still exercising the buggy code path.
SELECT id, round(L2DistanceTransposed(vec, if(id > 0, [0, 1, 2], NULL), 3), 4) AS dist
FROM qbit_test
ORDER BY id;

-- Same for `cosineDistanceTransposed`.
SELECT id, round(cosineDistanceTransposed(vec, if(id > 0, [1, 1, 1], NULL), 3), 4) AS dist
FROM qbit_test
ORDER BY id;

DROP TABLE qbit_test;
