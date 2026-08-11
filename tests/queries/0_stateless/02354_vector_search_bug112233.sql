-- Tags: no-fasttest, no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/112233
-- An integer reference vector such as [1, 2] denotes the same point as [1.0, 2.0], so it must use the index as well.

SET explain_query_plan_default = 'legacy';

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO tab VALUES
  (0, [1.0, 0.0]),
  (1, [1.1, 0.0]),
  (2, [1.2, 0.0]),
  (3, [1.3, 0.0]),
  (4, [1.4, 0.0]),
  (5, [0.0, 2.0]),
  (6, [0.0, 2.1]),
  (7, [0.0, 2.2]),
  (8, [0.0, 2.3]),
  (9, [0.0, 2.4]);

SELECT '-- Unsigned integer reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Signed integer reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [-1, -2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Mixed integer and float reference vector: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [0, 2.0])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Reference vector not exactly representable in Float32: index usage expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [16777217, 2])
    LIMIT 1
)
WHERE explain LIKE '%vector_similarity%';

SELECT '-- Integer and float spellings of the same reference vector return the same result';
SELECT id FROM tab ORDER BY L2Distance(vec, [0, 2]) LIMIT 1;
SELECT id FROM tab ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 1;

DROP TABLE tab;
