-- Tags: no-fasttest, no-parallel-replicas

-- Verify that vector search queries on multiple parts use lazy materialization

SET parallel_replicas_local_plan = 1; -- this setting is randomized, set it explicitly to force local plan for parallel replicas
SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id Int32, s1 String, s2 String, s3 String, x UInt32, vec Array(Float32)) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 128;
ALTER TABLE tab ADD INDEX idx_vec vec TYPE vector_similarity('hnsw', 'L2Distance', 2);

SYSTEM STOP MERGES tab;

-- 3 parts
INSERT INTO tab SELECT number, randomPrintableASCII(10), randomPrintableASCII(20), randomPrintableASCII(5), rand(), [randCanonical(), randCanonical()] FROM numbers(10000);
INSERT INTO tab SELECT number, randomPrintableASCII(10), randomPrintableASCII(20), randomPrintableASCII(5), rand(), [randCanonical(), randCanonical()] FROM numbers(10000);
INSERT INTO tab SELECT number, randomPrintableASCII(10), randomPrintableASCII(20), randomPrintableASCII(5), rand(), [randCanonical(), randCanonical()] FROM numbers(10000);

-- Verify that lazy materialization is used
SELECT trimLeft(explain) FROM
(
    EXPLAIN WITH [0.1, 0.2] AS reference_vec
    SELECT id, s1, s2, s3, x
    FROM tab
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 10
)
WHERE explain LIKE '%LazilyReadFromMergeTree%';

-- Verify that vector index is used
SELECT trimLeft(explain) FROM
(
    EXPLAIN indexes = 1 WITH [0.1, 0.2] AS reference_vec
    SELECT id, s1, s2, s3, x
    FROM tab
    ORDER BY L2Distance(vec, reference_vec) ASC
    LIMIT 10
)
WHERE explain LIKE '%idx_vec%';

-- Run the query and fetch rows from multiple parts
SELECT count(*) FROM
(
WITH [0.1, 0.2] AS reference_vec
SELECT id, s1, s2, s3, x
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10
);

DROP TABLE tab;
