-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-shared-merge-tree: no quorum, and SharedMergeTree pins sequential consistency in the storage snapshot instead

-- `vectorSearch` must respect `select_sequential_consistency`: parts that are not yet
-- written to the quorum of replicas must be hidden, the same as for a normal SELECT.
-- Modelled on 00732_quorum_insert_select_with_old_data_and_without_quorum_zookeeper_long.

SET send_logs_level = 'fatal';
SET allow_experimental_search_topk_table_functions = 1;
-- Async inserts do not make sense with non-parallel quorum inserts.
SET async_insert = 0;

DROP TABLE IF EXISTS quorum1;
DROP TABLE IF EXISTS quorum2;

CREATE TABLE quorum1
(
    id UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04354/seq_consistency', '1') ORDER BY id;

CREATE TABLE quorum2
(
    id UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04354/seq_consistency', '2') ORDER BY id;

INSERT INTO quorum1 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]);
INSERT INTO quorum1 VALUES (2, [0.0, 1.0]);

SYSTEM SYNC REPLICA quorum2;

SELECT '-- both replicas see all rows under sequential consistency';

SELECT id FROM vectorSearch(currentDatabase(), quorum1, idx, [1.0, 0.0], 10)
ORDER BY _score, id
SETTINGS select_sequential_consistency = 1;

SELECT id FROM vectorSearch(currentDatabase(), quorum2, idx, [1.0, 0.0], 10)
ORDER BY _score, id
SETTINGS select_sequential_consistency = 1;

SET insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 0;

SYSTEM STOP FETCHES quorum1;

INSERT INTO quorum2 VALUES (3, [1.05, 0.0]); -- { serverError UNKNOWN_STATUS_OF_INSERT }

SELECT '-- the part of the failed quorum insert is hidden from vectorSearch';

SELECT id FROM vectorSearch(currentDatabase(), quorum2, idx, [1.0, 0.0], 10)
ORDER BY _score, id
SETTINGS select_sequential_consistency = 1;

DROP TABLE quorum1;
DROP TABLE quorum2;
