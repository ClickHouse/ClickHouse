-- Error cases of NEAREST JOIN.

DROP TABLE IF EXISTS t_upload;
DROP TABLE IF EXISTS t_base;

CREATE TABLE t_base (k UInt32, vec Array(Float32), name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_base VALUES (1, [1.0, 0.0], 'a'), (1, [0.0, 1.0], 'b');

CREATE TABLE t_upload (id UInt32, k UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_upload VALUES (1, 1, [0.9, 0.1]);

SELECT 'USING is not supported';
SELECT id, name FROM t_upload NEAREST JOIN t_base USING (k); -- { serverError NOT_IMPLEMENTED }

SELECT 'a distance function is required';
SELECT id, name FROM t_upload NEAREST JOIN t_base ON t_upload.k = t_base.k; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'an equality predicate is required';
SELECT id, name FROM t_upload NEAREST JOIN t_base ON L2Distance(t_upload.vec, t_base.vec); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'only one distance function is allowed';
SELECT id, name FROM t_upload NEAREST JOIN t_base
    ON t_upload.k = t_base.k
        AND L2Distance(t_upload.vec, t_base.vec)
        AND cosineDistance(t_upload.vec, t_base.vec); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'the distance function must take one column from each side';
SELECT id, name FROM t_upload NEAREST JOIN t_base
    ON t_upload.k = t_base.k AND L2Distance(t_upload.vec, t_upload.vec); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'a constant join expression is rejected';
SELECT id, name FROM t_upload NEAREST JOIN t_base ON 1; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'a cross-side inequality in ON is rejected';
SELECT id, name FROM t_upload NEAREST JOIN t_base
    ON t_upload.k = t_base.k
        AND L2Distance(t_upload.vec, t_base.vec)
        AND t_upload.id != t_base.k; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'RIGHT and FULL kinds are rejected';
SELECT id, name FROM t_upload NEAREST RIGHT JOIN t_base ON t_upload.k = t_base.k AND L2Distance(t_upload.vec, t_base.vec); -- { clientError SYNTAX_ERROR }
SELECT id, name FROM t_upload NEAREST FULL JOIN t_base ON t_upload.k = t_base.k AND L2Distance(t_upload.vec, t_base.vec); -- { clientError SYNTAX_ERROR }

SELECT 'integer vectors are promoted to the floating point supertype';
SELECT id, name FROM t_upload NEAREST JOIN
    (SELECT k, CAST([1, 0], 'Array(UInt8)') AS vec, name FROM t_base) AS t_base_int
    ON t_upload.k = t_base_int.k AND L2Distance(t_upload.vec, t_base_int.vec)
ORDER BY id, name;

SELECT 'vectors without a native floating point supertype are rejected';
SELECT id, name FROM t_upload NEAREST JOIN
    (SELECT k, CAST(vec, 'Array(BFloat16)') AS vec, name FROM t_base) AS t_base_bfloat
    ON t_upload.k = t_base_bfloat.k
        AND L2Distance(CAST(t_upload.vec, 'Array(BFloat16)'), t_base_bfloat.vec); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 'nullable vector elements are rejected by the distance function';
SELECT id, name FROM t_upload NEAREST JOIN
    (SELECT k, CAST(vec, 'Array(Nullable(Float32))') AS vec, name FROM t_base) AS t_base_nullable
    ON t_upload.k = t_base_nullable.k AND L2Distance(t_upload.vec, t_base_nullable.vec); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'array sizes must match at execution time';
SELECT id, name FROM t_upload NEAREST JOIN
    (SELECT k, [1.0, 2.0, 3.0]::Array(Float32) AS vec, name FROM t_base) AS t_base_wide
    ON t_upload.k = t_base_wide.k AND L2Distance(t_upload.vec, t_base_wide.vec); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

SELECT 'the old analyzer is not supported';
SET enable_analyzer = 0;
SELECT id, name FROM t_upload NEAREST JOIN t_base ON t_upload.k = t_base.k AND L2Distance(t_upload.vec, t_base.vec); -- { serverError NOT_IMPLEMENTED }
SET enable_analyzer = 1;

DROP TABLE t_upload;
DROP TABLE t_base;
