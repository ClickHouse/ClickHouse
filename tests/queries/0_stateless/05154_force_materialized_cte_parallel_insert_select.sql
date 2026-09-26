-- Tags: zookeeper
-- zookeeper: the test creates `ReplicatedMergeTree` tables.

-- An `INSERT ... SELECT` whose `SELECT` declares a materialized CTE bypasses every route of
-- `parallel_distributed_insert_select`: the fast paths expand CTE references in place and the parallel-replicas
-- route forwards the resolved query, so both would evaluate the CTE per reference. The self-join inside one scalar
-- subquery compares two references to a `rand64()` CTE: `1` when evaluated once, `0` otherwise. A CTE with
-- `ORDER BY ... LIMIT` in `FROM` must not be distributed per replica. `WITH` aliases must keep resolving on every
-- route.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET force_materialized_cte = 1;
SET parallel_distributed_insert_select = 2;
SET distributed_foreground_insert = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS src_local_05154;
DROP TABLE IF EXISTS dst_local_05154;
DROP TABLE IF EXISTS src_dist_05154;
DROP TABLE IF EXISTS dst_dist_05154;
DROP TABLE IF EXISTS dst_rmt_05154 SYNC;
DROP TABLE IF EXISTS dst_x_05154;
DROP TABLE IF EXISTS dst_xd_05154;
DROP TABLE IF EXISTS dst_xr_05154 SYNC;
CREATE TABLE src_local_05154 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_local_05154 VALUES (1);
CREATE TABLE dst_local_05154 (same UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE src_dist_05154 AS src_local_05154 ENGINE = Distributed(test_shard_localhost, currentDatabase(), src_local_05154);
CREATE TABLE dst_dist_05154 AS dst_local_05154 ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst_local_05154);

SELECT 'materialized CTE bypasses the Distributed fast path (local shard)';
SET prefer_localhost_replica = 1;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
SELECT countIf(same = 0), count() FROM dst_local_05154;
TRUNCATE TABLE dst_local_05154;

SELECT 'materialized CTE bypasses the Distributed fast path (text route)';
SET prefer_localhost_replica = 0;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154;
SELECT countIf(same = 0), count() FROM dst_local_05154;
TRUNCATE TABLE dst_local_05154;

SELECT 'route evidence: a plain CTE is forwarded, a materialized CTE is not';
-- On the text route the fast path forwards `INSERT ... SELECT` to the shard as a secondary query.
TRUNCATE TABLE dst_local_05154;
INSERT INTO dst_dist_05154 WITH c AS (SELECT 1 AS x) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same_plain_05154 FROM src_dist_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same_materialized_05154 FROM src_dist_05154;
SYSTEM FLUSH LOGS query_log;
SELECT countIf(query LIKE '%same_plain_05154%') > 0, countIf(query LIKE '%same_materialized_05154%')
FROM system.query_log
WHERE event_date >= yesterday() AND is_initial_query = 0 AND type = 'QueryFinish' AND query LIKE 'INSERT INTO%SELECT%'
  AND has(databases, currentDatabase());
TRUNCATE TABLE dst_local_05154;
SET prefer_localhost_replica = 1;

SELECT 'materialized CTE bypasses the parallel replicas route (Distributed destination)';
SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1, parallel_replicas_insert_select_local_pipeline = 1, parallel_replicas_prefer_local_replica = 1;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
SELECT countIf(same = 0), count() FROM dst_local_05154;
TRUNCATE TABLE dst_local_05154;

SELECT 'materialized CTE bypasses the parallel replicas route (ReplicatedMergeTree destination)';
CREATE TABLE dst_rmt_05154 (same UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_rmt_05154', 'r1') ORDER BY tuple();
INSERT INTO dst_rmt_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_local_05154;
SELECT countIf(same = 0), count() FROM dst_rmt_05154;

SELECT 'nested declaration with inner SETTINGS bypasses the routes';
-- Declared inside the scalar subquery, enabled only there: detection must be recursive and not depend on the
-- session setting, otherwise the local shard or the parallel-replicas route evaluates the CTE twice.
SET enable_materialized_cte = 0;
TRUNCATE TABLE dst_local_05154;
TRUNCATE TABLE dst_rmt_05154;
SET enable_parallel_replicas = 0, prefer_localhost_replica = 1;
INSERT INTO dst_dist_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_dist_05154;
INSERT INTO dst_dist_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_dist_05154;
SELECT countIf(same = 0), count() FROM dst_local_05154;
TRUNCATE TABLE dst_local_05154;
SET enable_parallel_replicas = 1;
INSERT INTO dst_rmt_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_local_05154;
INSERT INTO dst_rmt_05154 SELECT (WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT a.x = b.x FROM c AS a, c AS b SETTINGS enable_materialized_cte = 1) AS same FROM src_local_05154;
SELECT countIf(same = 0), count() FROM dst_rmt_05154;
SET enable_materialized_cte = 1;
DROP TABLE dst_rmt_05154 SYNC;

SELECT 'a CTE with ORDER BY LIMIT in FROM is not distributed per replica';
-- Enough rows for several replicas to take part; exactly the globally smallest row must be inserted.
INSERT INTO src_local_05154 SELECT number + 10 FROM numbers(200000);
CREATE TABLE dst_x_05154 (x UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dst_xd_05154 AS dst_x_05154 ENGINE = Distributed(test_shard_localhost, currentDatabase(), dst_x_05154);
INSERT INTO dst_xd_05154 WITH c AS (SELECT x FROM src_local_05154 ORDER BY x LIMIT 1) SELECT x FROM c;
SELECT count(), min(x) FROM dst_x_05154;
TRUNCATE TABLE dst_x_05154;
CREATE TABLE dst_xr_05154 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst_xr_05154', 'r1') ORDER BY tuple();
INSERT INTO dst_xr_05154 WITH c AS (SELECT x FROM src_local_05154 ORDER BY x LIMIT 1) SELECT x FROM c;
SELECT count(), min(x) FROM dst_xr_05154;
DROP TABLE dst_xr_05154 SYNC;
SET enable_parallel_replicas = 0;
TRUNCATE TABLE src_local_05154;
INSERT INTO src_local_05154 VALUES (1);

SELECT 'materialization disabled: rejected, and inlined with the guard off';
SET enable_materialized_cte = 0;
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154; -- { serverError SUPPORT_IS_DISABLED }
INSERT INTO dst_dist_05154 WITH c AS MATERIALIZED (SELECT rand64() AS x FROM numbers(1)) SELECT (SELECT a.x = b.x FROM c AS a, c AS b) AS same FROM src_dist_05154 SETTINGS force_materialized_cte = 0, send_logs_level = 'fatal';
SELECT count() FROM dst_local_05154;
TRUNCATE TABLE dst_local_05154;
SET enable_materialized_cte = 1;

SELECT 'WITH aliases still resolve on every route';
-- The fast paths expand these forms in place before forwarding: a scalar alias used inside a nested subquery,
-- and a scalar alias as the right operand of IN.
SET prefer_localhost_replica = 1;
INSERT INTO dst_xd_05154 WITH 1 AS k SELECT x FROM src_dist_05154 WHERE x IN (SELECT number FROM numbers(3) WHERE number = k);
INSERT INTO dst_xd_05154 WITH [1, 2] AS ks SELECT x + 10 FROM src_dist_05154 WHERE x IN ks;
SELECT x FROM dst_x_05154 ORDER BY x;
TRUNCATE TABLE dst_x_05154;
SET prefer_localhost_replica = 0;
INSERT INTO dst_xd_05154 WITH 1 AS k SELECT x FROM src_dist_05154 WHERE x IN (SELECT number FROM numbers(3) WHERE number = k);
INSERT INTO dst_xd_05154 WITH [1, 2] AS ks SELECT x + 10 FROM src_dist_05154 WHERE x IN ks;
SELECT x FROM dst_x_05154 ORDER BY x;
TRUNCATE TABLE dst_x_05154;
SET prefer_localhost_replica = 1;

DROP TABLE dst_xd_05154;
DROP TABLE dst_x_05154;
DROP TABLE dst_dist_05154;
DROP TABLE src_dist_05154;
DROP TABLE dst_local_05154;
DROP TABLE src_local_05154;
