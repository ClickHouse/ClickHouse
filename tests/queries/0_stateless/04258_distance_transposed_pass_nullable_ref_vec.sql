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
