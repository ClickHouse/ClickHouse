-- Regression: when the reference-vector argument of the transposed distance functions has a type that makes the result
-- Nullable (e.g. a Variant), DistanceTransposedPartialReadsPass must keep the result Nullable after it rewrites the call
-- and casts the reference vector away. It used to drop the nullability and throw a logical error during analysis:
--   "FUNCTION query tree node does not have a valid source node after running DistanceTransposedPartialReadsPass.
--    Before: Nullable(Float64), after: Float64" (found by the AST fuzzer).
-- The pass previously only restored nullability coming from the precision / dims arguments, not the reference vector.

SET enable_analyzer = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS qbit_plain;
DROP TABLE IF EXISTS qbit_strided;
CREATE TABLE qbit_plain (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
CREATE TABLE qbit_strided (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]);
INSERT INTO qbit_strided VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

-- A Variant reference vector makes the original result Nullable. The rewritten result type must stay identical whether
-- or not the partial-reads optimization fires, for both distance functions and for the non-strided and strided forms.
SELECT 'Variant reference -> Nullable result, non-strided, optimization on';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 32)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(cosineDistanceTransposed(vec, ref, 32)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Same call with the optimization off (original result type for reference)';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 32)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 0;
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(cosineDistanceTransposed(vec, ref, 32)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT 'Variant reference -> Nullable result, strided 4-arg form, optimization on';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 32, 8)) FROM qbit_strided SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'A plain (non-Nullable) reference is unaffected: still Float64';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 32)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

-- Minimized shape of the original fuzzer query: the reference vector is a Variant built by multiIf. The optimization
-- must rewrite the call without throwing a logical error and keep the Nullable result type.
SELECT 'Minimized fuzzer shape: optimization must not throw a logical error';
WITH [toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS query_vec
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, multiIf(toFixedString(NULL, NULL), toNullable(isNotNull(toNullable(NULL))), query_vec), isNotNull(0))) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

DROP TABLE qbit_plain;
DROP TABLE qbit_strided;
