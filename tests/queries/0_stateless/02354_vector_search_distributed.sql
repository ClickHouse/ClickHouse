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
SELECT 
    id 
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, [1.0, 1.0])
LIMIT 3
SETTINGS log_comment='direct-query-on-remote', serialize_query_plan=0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    -- initiator must read the same amount of rows from the local table + data from remote
    -- 12 rows on the initiator (local table -- 6 rows and remote -- 6 rows)
    ProfileEvents['SelectedRows'] as SelectedRows -- 12 rows
FROM system.query_log
WHERE 
    current_database = currentDatabase() AND
    log_comment = 'direct-query-on-remote' AND
    type = 2 AND
    is_initial_query
FORMAT CSV;

SELECT '# Verify actual query results with remote()';
WITH [1.0, 1.0] AS reference_vec
SELECT DISTINCT id
FROM remote('127.{1,2}', currentDatabase(), tab_local)
ORDER BY L2Distance(vec, reference_vec)
LIMIT 5;

DROP TABLE tab_local SYNC;
