-- Tags: no-fasttest, no-ordinary-database
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110405
-- LIMIT ... WITH TIES should skip vector search optimization.

SET explain_query_plan_default = 'legacy';

SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES
  (1, [0.0, 1.0]),
  (2, [1.0, 0.0]),
  (3, [2.0, 1.0]),
  (4, [1.0, 2.0]),
  (5, [3.0, 1.0]),
  (6, [5.0, 5.0]);

SELECT '-- LIMIT WITH TIES: index usage not expected';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT id
    FROM tab
    ORDER BY L2Distance(vec, [1.0, 1.0])
    LIMIT 3 WITH TIES
)
WHERE explain LIKE '%vector_similarity%';

DROP TABLE tab;
