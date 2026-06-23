-- Regression test for `DistanceTransposedPartialReadsPass` with a null-bearing reference vector
-- (Nullable / Variant / Dynamic), broader than the always-NULL `Nullable(Nothing)` subcase that
-- `03829_distance_transposed_pass_nullable_nothing.sql` covers.

DROP TABLE IF EXISTS qbit_test;
CREATE TABLE qbit_test (id UInt64, vec QBit(Float32, 3)) ENGINE = Memory;
INSERT INTO qbit_test VALUES (1, [0,1,2]), (2, [1,2,3]), (3, [2,3,4]);

-- 1. The rewrite must not change the result: the optimized path (subcolumn reads) must produce the
--    same values and the same Nullable(Float64) type as the unoptimized path, including the NULL row.
SELECT id, round(L2DistanceTransposed(vec, if(id > 1, [0,1,2], NULL), 3), 4) AS dist
FROM qbit_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT id, round(L2DistanceTransposed(vec, if(id > 1, [0,1,2], NULL), 3), 4) AS dist
FROM qbit_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT id, round(cosineDistanceTransposed(vec, if(id > 1, [1,1,1], NULL), 3), 4) AS dist
FROM qbit_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT id, round(cosineDistanceTransposed(vec, if(id > 1, [1,1,1], NULL), 3), 4) AS dist
FROM qbit_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

-- 2. A nullable reference vector keeps the partial-read optimization: it must read the `vec.1`
--    subcolumn and never the whole `vec` column, and the result type must stay Nullable(Float64).
SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, if(id > 0, [0,1,2], NULL), 3) FROM qbit_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

SELECT toTypeName(L2DistanceTransposed(vec, if(id > 0, [0,1,2], NULL), 3)) FROM qbit_test LIMIT 1
SETTINGS optimize_qbit_distance_function_reads = 1;

-- 3. The optimization must survive randomized `short_circuit_function_evaluation`: the rewritten call
--    is evaluated on every row (even NULL ones), so it must not depend on short-circuiting.
SELECT id, round(L2DistanceTransposed(vec, if(id > 1, [0,1,2], NULL), 3), 4) AS dist
FROM qbit_test ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1, short_circuit_function_evaluation = 'disable';

-- 4. A nullable *precision* must also keep the optimization: its nullability is preserved on the
--    dimension constant, so the `vec.1` subcolumn is still read.
SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, [0,1,2], toNullable(3)) FROM qbit_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

-- 5. A *non-deterministic* nullable reference vector must NOT take the partial-read path: the
--    rewrite references the reference expression several times (the null mask, the `assumeNotNull`
--    value and the NULL-row dummy guard), so a value that can differ between evaluations would make
--    those references inconsistent. The optimization is skipped (the whole `vec` column is read, no
--    `vec.1` subcolumn) and the query still runs and returns Nullable(Float64).
SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, if(rand() % 2 = 0, [0,1,2]::Array(Float32), NULL), 3) FROM qbit_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

SELECT toTypeName(L2DistanceTransposed(vec, if(rand() % 2 = 0, [0,1,2]::Array(Float32), NULL), 3)) FROM qbit_test LIMIT 1
SETTINGS optimize_qbit_distance_function_reads = 1;

DROP TABLE qbit_test;

-- 6. The reference vector may be an expression-bearing `ColumnNode` (a table `ALIAS` column or an
--    array-join expression), not just a plain stored column. The rewrite clones that node; the clone
--    must keep the alias expression and its column dependency (here `id`) so the optimization still
--    reads the `vec.1` subcolumn, returns Nullable(Float64) and produces the same values as the
--    unoptimized path. A deterministic alias is materialized once, so it is safe to reference.
--    The alias type is an all-array `Variant` so the optimization still applies (see section 7).
DROP TABLE IF EXISTS qbit_alias_test;
CREATE TABLE qbit_alias_test
(
    id UInt64,
    vec QBit(Float32, 3),
    ref_alias Variant(Array(Float32)) ALIAS if(id > 1, [0,1,2]::Array(Float32), NULL)
) ENGINE = Memory
SETTINGS allow_experimental_qbit_type = 1, allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1;
INSERT INTO qbit_alias_test(id, vec) VALUES (1, [0,1,2]), (2, [1,2,3]), (3, [2,3,4]);

SELECT id, round(L2DistanceTransposed(vec, ref_alias, 3), 4) AS dist
FROM qbit_alias_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT id, round(L2DistanceTransposed(vec, ref_alias, 3), 4) AS dist
FROM qbit_alias_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, ref_alias, 3) FROM qbit_alias_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

SELECT toTypeName(L2DistanceTransposed(vec, ref_alias, 3)) FROM qbit_alias_test LIMIT 1
SETTINGS optimize_qbit_distance_function_reads = 1;

DROP TABLE qbit_alias_test;

-- 7. A `Variant` reference with a non-array alternative, or a `Dynamic` reference, can carry a
--    non-array value on a non-NULL row. The unoptimized function routes such a row through the
--    default Variant/Dynamic adaptor, which (with `*_throw_on_type_mismatch = 0`) yields NULL. The
--    optimized rewrite only masks on `isNull(ref)` and casts the value to `Array(element_type)`, so
--    it would mis-parse or fail on a non-array value. The optimization is therefore skipped for such
--    references (the whole `vec` column is read, no `vec.1` subcolumn) and the result matches the
--    unoptimized path row for row. An all-array `Variant` (every alternative is an array) is always
--    an array where it is not NULL, so it keeps the optimization.
SET allow_experimental_variant_type = 1, allow_suspicious_variant_types = 1, allow_experimental_dynamic_type = 1;
SET variant_throw_on_type_mismatch = 0, dynamic_throw_on_type_mismatch = 0;

DROP TABLE IF EXISTS qbit_mismatch_test;
CREATE TABLE qbit_mismatch_test
(
    id UInt64,
    vec QBit(Float32, 3),
    ref_var Variant(Array(Float32), String),
    ref_dyn Dynamic
) ENGINE = Memory;
INSERT INTO qbit_mismatch_test VALUES (1, [0,1,2], [0,1,2]::Array(Float32), [0,1,2]::Array(Float32)),
                                      (2, [1,2,3], '[0,1,2]', 'somestr'),
                                      (3, [2,3,4], NULL, 42);

-- The Variant-with-String reference: a String row must be NULL (not a parsed array), matching opt = 0.
SELECT id, L2DistanceTransposed(vec, ref_var, 3) AS dist
FROM qbit_mismatch_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT id, L2DistanceTransposed(vec, ref_var, 3) AS dist
FROM qbit_mismatch_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

-- The Dynamic reference: non-array rows must be NULL, matching opt = 0.
SELECT id, L2DistanceTransposed(vec, ref_dyn, 3) AS dist
FROM qbit_mismatch_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT id, L2DistanceTransposed(vec, ref_dyn, 3) AS dist
FROM qbit_mismatch_test ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

-- The optimization is skipped for these references: the whole `vec` column is read, no `vec.1`.
SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, ref_var, 3) FROM qbit_mismatch_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

SELECT countIf(explain ILIKE '%`vec.1`%') > 0 AS reads_subcolumns
FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1
      SELECT L2DistanceTransposed(vec, ref_dyn, 3) FROM qbit_mismatch_test
      SETTINGS optimize_qbit_distance_function_reads = 1);

DROP TABLE qbit_mismatch_test;
