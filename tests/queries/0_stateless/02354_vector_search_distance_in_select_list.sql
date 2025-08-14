-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas
--- no-parallel-replicas: Because the test records and verifies
--- _distance values returned from the rescoring optimization.

-- Tests that the rescoring optimization works when distance function is
-- present explicitly in the SELECT columns list, apart from ORDER BY.
-- The return type of the cosineDistance/L2Distance function will vary
-- based on the data type of the 2 input arguments.

SET enable_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET vector_search_with_rescoring = 0;

SELECT 'Creating tables with Array(Float32) and Array(BFloat16) column';
DROP TABLE IF EXISTS tab_f32;
CREATE TABLE tab_f32(
    id Int32,
    vec Array(Float32),
    INDEX vector_index vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

DROP TABLE IF EXISTS tab_bf16;
CREATE TABLE tab_bf16(
    id Int32,
    vec Array(BFloat16),
    INDEX vector_index vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;


INSERT INTO tab_f32 VALUES (0, [1.0, 0.0]),
                       (1, [1.1, 0.0]),
                       (2, [1.2, 0.0]),
                       (3, [1.3, 0.0]),
                       (4, [1.4, 0.0]),
                       (5, [0.0, 2.0]),
                       (6, [0.0, 2.1]),
                       (7, [0.0, 2.2]),
                       (8, [0.0, 2.3]),
                       (9, [0.0, 2.4]);

INSERT INTO tab_bf16 VALUES (0, [1.0, 0.0]),
                        (1, [1.1, 0.0]),
                        (2, [1.2, 0.0]),
                        (3, [1.3, 0.0]),
                        (4, [1.4, 0.0]),
                        (5, [0.0, 2.0]),
                        (6, [0.0, 2.1]),
                        (7, [0.0, 2.2]),
                        (8, [0.0, 2.3]),
                        (9, [0.0, 2.4]);

-- The nearest neighbours to [0.0, 0.2] are 5,6,7,8
-- In ClickHouse, an array like [0.0, 0.2] is of type Array(Float64)

SELECT 'Column is Array(Float32), Search Vector is Array(Float64)';
WITH [0.0, 2.0] AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_f32
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

SELECT 'Column is Array(Float32), Search Vector is Array(Float32)';
WITH (SELECT vec FROM tab_f32 WHERE id = 5) AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_f32
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

SELECT 'Column is Array(Float32), Search Vector is Array(BFloat16)';
WITH CAST([0.0, 2.0] AS Array(BFloat16)) AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_f32
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

DROP TABLE tab_f32;

SELECT 'Repeat the tests with the Array(BFloat16) column table';

SELECT 'Column is Array(BFloat16), Search Vector is Array(Float64)';
WITH [0.0, 2.0] AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_bf16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

SELECT 'Column is Array(BFloat16), Search Vector is Array(BFloat16)';
WITH (SELECT vec FROM tab_bf16 WHERE id = 5) AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_bf16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

SELECT 'Column is Array(BFloat16), Search Vector is Array(Float32)';
WITH CAST([0.0, 2.0] AS Array(Float32)) AS reference_vec
SELECT id, L2Distance(vec, reference_vec)
FROM tab_bf16
ORDER BY L2Distance(vec, reference_vec)
LIMIT 4;

DROP TABLE tab_bf16;
