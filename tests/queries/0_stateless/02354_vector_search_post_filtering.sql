-- Tags: no-fasttest, no-ordinary-database
-- Tests for vector search and post-filtering

SET allow_experimental_vector_similarity_index = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan=1; -- this setting is randomized, set it explicitly to have local plan for parallel replicas

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    dt Date,
    id Int32,
    attr1 Int32,
    attr2 Int32,
    vector Array(Float32),
    INDEX attr1_index attr1 TYPE minmax,
    INDEX vector_index vector TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 10000
)
ENGINE = MergeTree
PARTITION BY (dt)
ORDER BY (id)
SETTINGS index_granularity=3;

INSERT INTO tab VALUES
  ('2025-01-01', 1, 101, 1001, [1.0, 0.0]),
  ('2025-01-01', 2, 102, 1002, [1.1, 0.0]),
  ('2025-01-01', 3, 103, 1003, [1.2, 0.0]),
  ('2025-01-02', 4, 104, 1003, [1.3, 0.0]),
  ('2025-01-02', 5, 105, 1004, [1.4, 0.0]),
  ('2025-01-02', 6, 106, 1005, [1.5, 0.0]),
  ('2025-01-03', 7, 107, 1005, [1.6, 0.0]),
  ('2025-01-03', 8, 108, 1006, [1.7, 0.0]),
  ('2025-01-03', 9, 109, 1007, [1.8, 0.0]),
  ('2025-01-03', 10, 110, 1008, [1.9, 0.0]),
  ('2025-01-03', 11, 111, 1009, [2.0, 0.0]),
  ('2025-01-03', 12, 112, 1010, [2.1, 0.0]);

SET ann_prefer_pre_filtering = 0; -- default is 0

-- Base case - no predicates, vector index will be used and 3 out of 4 granules will be shortlisted from all the partitions
SELECT count(explain) FROM (
EXPLAIN indexes=1
SELECT id
FROM tab
ORDER BY L2Distance(vector, [1.0, 1.0])
LIMIT 2
)
WHERE explain LIKE '%vector_similarity%' OR explain LIKE '%Granules: 3/4%';

-- Additional predicate on partition key, 2 "full parts" selected: Vector index will be used
SELECT count(explain) FROM (
EXPLAIN indexes=1
SELECT id
FROM tab
WHERE dt <= '2025-01-02'
ORDER BY L2Distance(vector, [1.0, 1.0])
LIMIT 2
)
WHERE explain LIKE '%vector_similarity%';

-- Additional predicates present, 2 "full parts" selected by partition key, vector index
-- will be used and then filter on "attr1" will be evaluated.
SELECT count(explain) FROM (
EXPLAIN indexes=1
SELECT id
FROM tab
WHERE dt = '2025-01-03'
AND attr1 = 110
ORDER BY L2Distance(vector, [1.0, 1.0])
LIMIT 2
)
WHERE explain LIKE '%vector_similarity%';

-- Additional predicates present, 2 "full parts" selected by partition key, 1 part partially
-- selected by PK, vector index will not be used
SELECT id
FROM tab
WHERE dt = '2025-01-03'
AND id <= 9
ORDER BY L2Distance(vector, [1.0, 1.0])
LIMIT 2
SETTINGS log_comment = '02354_vector_search_post_filtering_query1';

SYSTEM FLUSH LOGS query_log;

SELECT DISTINCT ProfileEvents['USearchSearchCount']
FROM system.query_log
WHERE log_comment = '02354_vector_search_post_filtering_query1'
AND type = 'QueryFinish';
