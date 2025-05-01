-- Tags: no-fasttest, no-ordinary-database

set allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab1;
DROP TABLE IF EXISTS tab2;

CREATE TABLE tab1(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab1 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

CREATE TABLE tab2(id Int32, vec Array(BFloat16), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO tab2 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

DROP FUNCTION IF EXISTS embed_f32;
CREATE FUNCTION embed_f32 AS (x) -> [toFloat32(0.0), toFloat32(2.0)];

DROP FUNCTION IF EXISTS embed_bf16;
CREATE FUNCTION embed_bf16 AS (x) -> [toBFloat16(0.0), toBFloat16(2.0)];

SELECT id
FROM tab1
ORDER BY L2Distance(vec, (select vec from tab1 where id = 5))
LIMIT 1;

SELECT id
FROM tab2
ORDER BY L2Distance(vec, (select vec from tab2 where id = 5))
LIMIT 1;

SELECT id
FROM tab1
ORDER BY L2Distance(vec, embed_f32('dummy'))
LIMIT 1;

SELECT id
FROM tab2
ORDER BY L2Distance(vec, embed_bf16('dummy'))
LIMIT 1;

-- Verify that vector index is used in all variants
SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes=1
SELECT id
FROM tab1
ORDER BY L2Distance(vec, (select vec from tab1 where id = 5))
LIMIT 1
)
WHERE explain like '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes=1
SELECT id
FROM tab2
ORDER BY L2Distance(vec, (select vec from tab2 where id = 5))
LIMIT 1
)
WHERE explain like '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes=1
SELECT id
FROM tab1
ORDER BY L2Distance(vec, embed_f32('dummy'))
LIMIT 1
)
WHERE explain like '%vector_similarity%';

SELECT trimLeft(explain) AS explain FROM (
EXPLAIN indexes=1
SELECT id
FROM tab2
ORDER BY L2Distance(vec, embed_bf16('dummy'))
LIMIT 1
)
WHERE explain like '%vector_similarity%';
