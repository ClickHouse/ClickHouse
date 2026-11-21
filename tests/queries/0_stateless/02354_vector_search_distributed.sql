-- Tags: no-fasttest, no-ordinary-database, no-parallel-replicas, shard

-- Tests vector search with Distributed tables

SET enable_analyzer = 1;
SET prefer_localhost_replica = 1;
SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100000;

-- Create local table with vector similarity index
DROP TABLE IF EXISTS tab_local SYNC;
CREATE TABLE tab_local
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab_local VALUES
  (1, [1.0, 0.0]),
  (2, [1.1, 0.0]),
  (3, [1.2, 0.0]),
  (4, [1.3, 0.0]),
  (5, [1.4, 0.0]),
  (6, [1.5, 0.0]),
  (7, [1.6, 0.0]),
  (8, [1.7, 0.0]),
  (9, [1.8, 0.0]),
  (10, [1.9, 0.0]),
  (11, [2.0, 0.0]),
  (12, [2.1, 0.0]);


SELECT '# Direct query on local table - expect index usage';
EXPLAIN indexes = 1
SELECT
    id
FROM tab_local
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS force_data_skipping_indices = 'idx';

-- Disable lazy materialization for this query to get a stable SelectedRows count.
SELECT
    id
FROM tab_local
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS log_comment='direct-query-local-table', query_plan_optimize_lazy_materialization = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['SelectedRows'] as SelectedRows -- 1 vector similarity granule -> 2 merge tree granules -> 6 rows
FROM system.query_log
WHERE
    current_database = currentDatabase() AND
    event_date >= yesterday() AND event_time >= now() - 600 AND
    log_comment = 'direct-query-local-table' AND
    type = 2
ORDER BY event_time_microseconds
DESC LIMIT 1;

SELECT '# Direct query on remote() - expect index usage';
EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;
SELECT '# Verify actual query results with remote()';
WITH [1.0, 1.0] AS reference_vec
SELECT id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 5
SETTINGS force_data_skipping_indices = 'idx';

SELECT '# Distributed query with WHERE clause - expect index usage with filters detected';
EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
WHERE id > 3
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS force_data_skipping_indices = 'idx';

SELECT '# Distance function in SELECT but NOT in ORDER BY via remote() - must NOT use vector search index';
-- Disable lazy materialization: L2Distance in SELECT interacts non-deterministically with it.
EXPLAIN indexes = 1
SELECT
    id, L2Distance(vec, [1.0, 1.0]) as dist
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY id
LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT '# Table without vector similarity index via remote() - must NOT use vector search index';
DROP TABLE IF EXISTS tab_no_idx SYNC;
CREATE TABLE tab_no_idx
(
    id Int32,
    vec Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab_no_idx VALUES
  (1, [1.0, 0.0]),
  (2, [1.1, 0.0]),
  (3, [1.2, 0.0]),
  (4, [1.3, 0.0]),
  (5, [1.4, 0.0]),
  (6, [1.5, 0.0]);

EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_no_idx)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;

DROP TABLE tab_no_idx SYNC;

SELECT '# cosineDistance via remote() - expect index usage';
DROP TABLE IF EXISTS tab_cosine SYNC;
CREATE TABLE tab_cosine
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 2) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab_cosine VALUES
  (1, [1.0, 0.0]),
  (2, [0.0, 1.0]),
  (3, [1.0, 1.0]),
  (4, [0.0, -1.0]),
  (5, [-1.0, 0.0]),
  (6, [-1.0, -1.0]),
  (7, [1.0, -1.0]),
  (8, [-1.0, 1.0]),
  (9, [0.5, 0.5]),
  (10, [-0.5, 0.5]),
  (11, [0.5, -0.5]),
  (12, [-0.5, -0.5]);

EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_cosine)
ORDER BY cosineDistance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS force_data_skipping_indices = 'idx';

SELECT '# dotProduct via remote() - expect index usage (DESC sort)';
DROP TABLE IF EXISTS tab_dot SYNC;
CREATE TABLE tab_dot
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 2) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 3;

INSERT INTO tab_dot VALUES
  (1, [1.0, 0.0]),
  (2, [0.0, 1.0]),
  (3, [1.0, 1.0]),
  (4, [0.0, -1.0]),
  (5, [-1.0, 0.0]),
  (6, [-1.0, -1.0]),
  (7, [1.0, -1.0]),
  (8, [-1.0, 1.0]),
  (9, [0.5, 0.5]),
  (10, [-0.5, 0.5]),
  (11, [0.5, -0.5]),
  (12, [-0.5, -0.5]);

EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_dot)
ORDER BY dotProduct(vec, [1.0, 1.0]) DESC
LIMIT 3
SETTINGS force_data_skipping_indices = 'idx';

SELECT '# serialize_query_plan path - vector index must be used on the remote shard';
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS
    log_comment = 'serialize-query-plan-vector-search',
    serialize_query_plan = 1,
    prefer_localhost_replica = 1,
    query_plan_optimize_lazy_materialization = 0,
    force_data_skipping_indices = 'idx'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The initiator fans out to two shards (local + remote), each reading 6 rows with 2 marks
SELECT
    is_initial_query, ProfileEvents['SelectedRows'] as SelectedRows, ProfileEvents['SelectedMarks'] as SelectedMarks
FROM system.query_log
WHERE
    initial_query_id = (
        SELECT query_id
        FROM system.query_log
        WHERE
            current_database = currentDatabase() AND
            event_date >= yesterday() AND event_time >= now() - 600 AND
            log_comment = 'serialize-query-plan-vector-search' AND
            type = 2 AND
            is_initial_query = 1
            ORDER BY event_time_microseconds DESC
            LIMIT 1
    ) AND
    type = 2
ORDER BY is_initial_query DESC;

SELECT '# additional_result_filter must not cause HNSW overfetch';
-- The Planner pushes additional_result_filter into Union children, producing
-- Filter -> Limit -> Sorting -> Expression -> ReadFromMergeTree. The FilterStep is above
-- SortingStep and is NOT a read-time filter. Without the fix, the post-order outer match
-- (Union path) overwrites VectorSearchParameters with additional_filters_present = true,
-- causing HNSW to overfetch (SelectedRows = 18, SelectedMarks = 4 at multiplier 10).
-- With the fix, additional_filters_present stays false and SelectedRows = 12, SelectedMarks = 2.
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS
    log_comment = 'additional-result-filter-overfetch',
    additional_result_filter = '1=1',
    vector_search_index_fetch_multiplier = 10.0,
    serialize_query_plan = 1,
    prefer_localhost_replica = 1,
    query_plan_optimize_lazy_materialization = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    is_initial_query, ProfileEvents['SelectedMarks'] as SelectedMarks
FROM system.query_log
WHERE
    initial_query_id = (
        SELECT query_id
        FROM system.query_log
        WHERE
            current_database = currentDatabase() AND
            event_date >= yesterday() AND event_time >= now() - 600 AND
            log_comment = 'additional-result-filter-overfetch' AND
            type = 2 AND
            is_initial_query = 1
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) AND
    type = 2
ORDER BY is_initial_query DESC;

SELECT '# _distance column via remote() - must throw ILLEGAL_COLUMN';
SELECT id, _distance
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3; -- { serverError ILLEGAL_COLUMN }

SELECT '# Distributed engine with test_cluster_two_shards_localhost (issue #106397)';
DROP TABLE IF EXISTS tab_dist SYNC;
CREATE TABLE tab_dist AS tab_local
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), tab_local, rand());

EXPLAIN indexes = 1
SELECT
    id
FROM tab_dist
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;

SELECT
    id
FROM tab_dist
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab_dist SYNC;

DROP TABLE tab_cosine SYNC;
DROP TABLE tab_dot SYNC;
DROP TABLE tab_local SYNC;
