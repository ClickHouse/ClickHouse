-- Atomic CREATE MATERIALIZED VIEW ... POPULATE must read the source from the pinned local snapshot even
-- when parallel-replica / distributed insert-select settings are enabled on the query. The pinned
-- snapshot lives only in this server's contexts, so if the internal INSERT ... SELECT were dispatched to
-- remote replicas or through a distributed write it would read a fresh snapshot (breaking the exactly-once
-- cut) or re-send INSERT INTO the just-created view on other replicas. The population must therefore force
-- the local pinned-snapshot path. This test enables those settings and checks the backfill is exact.

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_only_with_analyzer = 0; -- necessary for CI run with disabled analyzer
SET parallel_distributed_insert_select = 2;

-- 1) Non-replicated MergeTree source with parallel replicas allowed for it.
SET parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS mv;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number FROM numbers(1000);
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
SELECT 'mergetree_pr', count(), uniqExact(id), sum(id) FROM mv;
DROP TABLE mv;
DROP TABLE src;

-- 2) ReplicatedMergeTree source (the engine family the finding is specifically about).
SET parallel_replicas_for_non_replicated_merge_tree = 0;

DROP TABLE IF EXISTS src_repl;
DROP TABLE IF EXISTS mv;
CREATE TABLE src_repl (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04494_src', 'r1') ORDER BY id;
INSERT INTO src_repl SELECT number FROM numbers(1000);
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src_repl;
SELECT 'replicated_pr', count(), uniqExact(id), sum(id) FROM mv;
DROP TABLE mv;
DROP TABLE src_repl SYNC;
