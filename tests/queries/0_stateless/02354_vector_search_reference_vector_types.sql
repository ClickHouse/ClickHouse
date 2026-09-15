-- Tags: no-fasttest, no-ordinary-database, no-parallel
-- no-parallel: SQL functions are not per-database, they are global

-- Tests that vector search queries work with reference vectors of different data types.

SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab_f64;
DROP TABLE IF EXISTS tab_f32;
DROP TABLE IF EXISTS tab_bf16;

SELECT 'Create tables with vector similarity indexs on Float64, Float32 and BFloat16 columns';

CREATE TABLE tab_f64(id Int32, vec Array(Float64), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_f64 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

CREATE TABLE tab_f32(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_f32 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

CREATE TABLE tab_bf16(id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab_bf16 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

DROP FUNCTION IF EXISTS constF64;
CREATE FUNCTION constF64 AS () -> [toFloat64(0.0), toFloat64(2.0)];

DROP FUNCTION IF EXISTS constF32;
CREATE FUNCTION constF32 AS () -> [toFloat32(0.0), toFloat32(2.0)];

DROP FUNCTION IF EXISTS constBF16;
CREATE FUNCTION constBF16 AS () -> [toBFloat16(0.0), toBFloat16(2.0)];

SELECT 'Run all combinations of vector search queries: column type x reference vector type';

SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constF64())
LIMIT 1;

SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constF32())
LIMIT 1;

SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constBF16())
LIMIT 1;

SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constF64())
LIMIT 1;

SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constF32())
LIMIT 1;

SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constBF16())
LIMIT 1;

SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constF64())
LIMIT 1;

SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constF32())
LIMIT 1;

SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constBF16())
LIMIT 1;

SELECT 'Check that the index is used for all combinations of vector search queries: column type x reference vector type';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constF64())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constF32())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f64
ORDER BY L2Distance(vec, constBF16())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constF64())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constF32())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, constBF16())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constF64())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constF32())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_bf16
ORDER BY L2Distance(vec, constBF16())
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

DROP FUNCTION constF64;
DROP FUNCTION constF32;
DROP FUNCTION constBF16;

SELECT 'Check that non-const reference vectors also work';

DROP FUNCTION IF EXISTS nonConstF32;
CREATE FUNCTION nonConstF32 AS (arg1) -> (SELECT [toFloat32((arg1 % 10)/10), toFloat32((arg1 % 10)/10)]);

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes = 1
SELECT id
FROM tab_f32
ORDER BY L2Distance(vec, nonConstF32(rand()))
LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';
DROP FUNCTION nonConstF32;

DROP TABLE tab_f64;
DROP TABLE tab_f32;
DROP TABLE tab_bf16;

