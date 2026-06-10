-- Regression test for `DistanceTransposedPartialReadsPass`.
--
-- 1. It must not throw a logical error when the reference vector can carry NULL
--    (Nullable / Variant / Dynamic), which is broader than the `Nullable(Nothing)`
--    subcase already covered by `03829_distance_transposed_pass_nullable_nothing.sql`.
-- 2. It must still apply the partial-read optimization when only the precision
--    argument is nullable, because that nullability is preserved on the dimension
--    constant rather than stripped from the reference vector.

DROP TABLE IF EXISTS qbit_test;
CREATE TABLE qbit_test (id UInt64, vec QBit(Float32, 3)) ENGINE = Memory;
INSERT INTO qbit_test VALUES (1, [0,1,2]), (2, [1,2,3]), (3, [2,3,4]);

-- `if` makes the reference vector a `Variant(Array(...))`, so the function returns
-- `Nullable(Float64)`. Casting it to a non-nullable `Array(...)` used to abort with
-- `Before: Nullable(Float64), after: Float64` (STID 0250-3bee). Using `id > 0` as the
-- predicate keeps the result non-null on every row, so the output stays deterministic.
SELECT id, round(L2DistanceTransposed(vec, if(id > 0, [0, 1, 2], NULL), 3), 4) AS dist
FROM qbit_test
ORDER BY id;

-- Same for `cosineDistanceTransposed`.
SELECT id, round(cosineDistanceTransposed(vec, if(id > 0, [1, 1, 1], NULL), 3), 4) AS dist
FROM qbit_test
ORDER BY id;

-- A nullable *precision* must NOT block the optimization: the rewrite keeps the result
-- nullable by forcing the dimension constant nullable, so partial reads of `vec.1`,
-- `vec.2`, `vec.3` are still used (the reference vector is non-null here).
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
SELECT L2DistanceTransposed(vec, [0, 1, 2], toNullable(3)) FROM qbit_test
SETTINGS optimize_qbit_distance_function_reads = 1;

-- A nullable *reference vector* must skip the optimization: the cast would strip
-- nullability, so the whole `vec` column is read instead of the subcolumns.
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
SELECT L2DistanceTransposed(vec, if(id > 0, [0, 1, 2], NULL), 3) FROM qbit_test
SETTINGS optimize_qbit_distance_function_reads = 1;

DROP TABLE qbit_test;
