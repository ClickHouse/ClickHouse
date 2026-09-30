-- Tags: no-fasttest, no-ordinary-database

-- Incident ClickHouse/clickhouse-core-incidents#1654
--
-- The `_distance` virtual column is internal to the vector search optimization
-- and must not be referenced directly in queries. Doing so previously caused a
-- LOGICAL_ERROR ("Vector column unexpectedly already replaced"). Now it throws
-- a user-facing ILLEGAL_COLUMN error.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    embedding Array(Float32),
    INDEX idx embedding TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [0.0, 2.0]), (6, [0.0, 2.1]), (7, [0.0, 2.2]), (8, [0.0, 2.3]), (9, [0.0, 2.4]);

-- Directly selecting `_distance` alongside a vector search ORDER BY should throw.
SELECT id, _distance
FROM tab
ORDER BY L2Distance(embedding, [1.0, 0.0])
LIMIT 3; -- { serverError ILLEGAL_COLUMN }

SELECT _distance
FROM tab
ORDER BY cosineDistance(embedding, [1.0, 0.0])
LIMIT 3; -- { serverError ILLEGAL_COLUMN }

DROP TABLE tab;
