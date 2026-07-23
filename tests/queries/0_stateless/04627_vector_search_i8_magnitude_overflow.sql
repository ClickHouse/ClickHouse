-- Tags: no-fasttest, no-ordinary-database

-- With i8 quantization, vectors whose squared magnitude overflows to infinity used to cause undefined behavior in usearch:
-- its i8 cast scales each element as x * 127.0 / sqrt(sum(x^2)), and inf/inf is NaN, which is then cast to int8.
-- Such vectors must be rejected, like non-finite and zero-magnitude vectors.
-- Issue: https://github.com/ClickHouse/ClickHouse/issues/111621

DROP TABLE IF EXISTS tab;

-- Only Float64 vectors can overflow the double magnitude accumulator.
CREATE TABLE tab (id Int32, vec Array(Float64), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3, 'i8', 32, 128))
ENGINE = MergeTree ORDER BY id;

-- In INSERT
INSERT INTO tab VALUES (1, [1e307, 1.0, 1.0]); -- { serverError INCORRECT_DATA }

-- Insert dummy values, otherwise the SELECT earlies out before the magnitude check
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]);

-- In reference vector
SELECT id FROM tab ORDER BY L2Distance(vec, [1e307, 1.0, 1.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab;

-- The reference vector is Float64 regardless of the column type, so the search path must be guarded for Float32 columns too.
CREATE TABLE tab (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3, 'i8', 32, 128))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]);

SELECT id FROM tab ORDER BY L2Distance(vec, [1e307, 1.0, 1.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

DROP TABLE tab;
