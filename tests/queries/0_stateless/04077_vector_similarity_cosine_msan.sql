-- Tags: no-fasttest

-- Reproducer for MSan use-of-uninitialized-value in simsimd_cos_f32_sve
-- https://github.com/ClickHouse/ClickHouse/issues/101232

DROP TABLE IF EXISTS test_vec;

CREATE TABLE test_vec
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO test_vec VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [0.0, 2.0]), (4, [0.0, 2.1]);

WITH [0.0, 2.0] AS ref
SELECT id
FROM test_vec
ORDER BY cosineDistance(vec, ref), id
LIMIT 3;

DROP TABLE test_vec;
