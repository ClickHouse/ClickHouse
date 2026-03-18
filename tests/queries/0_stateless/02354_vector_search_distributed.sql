-- Tags: no-fasttest, no-ordinary-database, shard

-- Tests vector search with Distributed tables

SET enable_analyzer = 1;
SET prefer_localhost_replica = 1;

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
LIMIT 3;

SELECT
    id
FROM tab_local
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS log_comment='direct-query-local-table'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['SelectedRows'] as SelectedRows -- 1 vector similarity granule -> 2 merge tree granules -> 6 rows
FROM system.query_log
WHERE
    current_database = currentDatabase() AND -- can use current_database, because there are no remote queries
    log_comment = 'direct-query-local-table' AND
    type = 2;

SELECT '# Direct query on remote() - expect index usage';
EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;
SELECT '# Verify actual query results with remote()';
WITH [1.0, 1.0] AS reference_vec
SELECT DISTINCT id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, reference_vec)
LIMIT 5;

SELECT '# Distributed query with WHERE clause - expect index usage with filters detected';
EXPLAIN indexes = 1
SELECT
    id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
WHERE id > 3
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3;

SELECT '# Distance function in SELECT but NOT in ORDER BY via remote() - must NOT use vector search index';
EXPLAIN indexes = 1
SELECT
    id, L2Distance(vec, [1.0, 1.0]) as dist
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY id
LIMIT 3;

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
DROP TABLE tab_local SYNC;
