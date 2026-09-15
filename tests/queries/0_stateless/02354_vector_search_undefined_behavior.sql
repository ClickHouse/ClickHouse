-- Tags: no-fasttest, no-ordinary-database

DROP TABLE IF EXISTS tab;

-- Tests that NaN and +-Inf values in the reference vector and in the data are rejected.
-- The presence of such values causes undefined behavior in usearch.

-- Test with Float32 and with BFloat16. The latter internally exercises a special path.

CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3))
ENGINE = MergeTree ORDER BY id;

-- In INSERT
INSERT INTO tab VALUES (1, [toFloat32(nan), toFloat32(1.0), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }
INSERT INTO tab VALUES (1, [toFloat32(+inf), toFloat32(1.0), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }
INSERT INTO tab VALUES (1, [toFloat32(-inf), toFloat32(1.0), toFloat32(1.0)]); -- { serverError INCORRECT_DATA }

-- Insert dummy values, otherwise the SELECT earlies out before the nan/inf check
INSERT INTO tab VALUES (1, [1.0, 1.0, 1.0]);

-- In reference vector
SELECT id FROM tab ORDER BY L2Distance(vec, [toFloat32(nan), toFloat32(1.0), toFloat32(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }
SELECT id FROM tab ORDER BY L2Distance(vec, [toFloat32(+inf), toFloat32(1.0), toFloat32(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }
SELECT id FROM tab ORDER BY L2Distance(vec, [toFloat32(-inf), toFloat32(1.0), toFloat32(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab;

CREATE TABLE tab (id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3))
ENGINE = MergeTree ORDER BY id;

-- In INSERT
INSERT INTO tab VALUES (1, [toBFloat16(nan), toBFloat16(1.0), toBFloat16(1.0)]); -- { serverError INCORRECT_DATA }
INSERT INTO tab VALUES (1, [toBFloat16(+inf), toBFloat16(1.0), toBFloat16(1.0)]); -- { serverError INCORRECT_DATA }
INSERT INTO tab VALUES (1, [toBFloat16(-inf), toBFloat16(1.0), toBFloat16(1.0)]); -- { serverError INCORRECT_DATA }

-- Insert dummy values, otherwise the SELECT earlies out before the nan/inf check
INSERT INTO tab VALUES (1, [1.0, 1.0, 1.0]);

-- In reference vector
SELECT id FROM tab ORDER BY L2Distance(vec, [toBFloat16(nan), toBFloat16(1.0), toBFloat16(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }
SELECT id FROM tab ORDER BY L2Distance(vec, [toBFloat16(+inf), toBFloat16(1.0), toBFloat16(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }
SELECT id FROM tab ORDER BY L2Distance(vec, [toBFloat16(-inf), toBFloat16(1.0), toBFloat16(1.0)]) LIMIT 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab;

-- Test another special case for i8 quantization. Zero-magnitude vectors (searched data and reference vectors)
-- also cause undefined behavior in ubsan --> reject them

CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3, 'i8', 32, 128))
ENGINE = MergeTree ORDER BY id;

-- There are other zero-magnitude vectors, but we can't test the other ones reliably because of rounding errors
INSERT INTO tab VALUES (12, [0.0, 0.0, 0.0]); -- { serverError INCORRECT_DATA }

-- Insert dummy values, otherwise the SELECT earlies out before the zero-magnitude check
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]);

SELECT id FROM tab ORDER BY L2Distance(vec, [0.0, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab;
