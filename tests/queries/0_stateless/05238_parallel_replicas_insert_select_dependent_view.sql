-- With `parallel_distributed_insert_select = 2` every replica runs the whole INSERT over its own slice of
-- the coordinated read, and pushes that slice through the target's dependent materialized views. A view
-- target that does not replicate therefore keeps a different subset of the rows on each replica, and a
-- later read of the view from one replica misses the rows the others inserted. The distributed write is
-- refused whenever a dependent view is reachable, and where the reachable set is not enumerable here at
-- all (an `Alias` target), while a concrete replicated target without views keeps it (the first arm below).
-- The last arm is the shape issue #121169 reported: a sink that does not replicate at all, where every
-- replica resolves the same destination and only one replica's share of the rows survives.

SET enable_analyzer = 1; -- parallel distributed insert select for replicated tables works only with analyzer
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS src;
CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO src SELECT number FROM numbers(30000);

-- 1) No dependent view: the distributed write is used. Without this arm the two below also pass when
-- nothing distributes the write at all.
DROP TABLE IF EXISTS dst_plain SYNC;
CREATE TABLE dst_plain (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_plain', 'r1') ORDER BY k;

INSERT INTO dst_plain SELECT k FROM src SETTINGS log_comment = '05238_plain';

SELECT 'no view: rows', count() FROM dst_plain;
SYSTEM FLUSH LOGS query_log;
SELECT 'no view: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_plain'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- 2) A dependent view whose target does not replicate: the write stays on the initiator.
DROP TABLE IF EXISTS dst_view SYNC;
CREATE TABLE dst_view (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_view', 'r1') ORDER BY k;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY k AS SELECT k FROM dst_view;

INSERT INTO dst_view SELECT k FROM src SETTINGS log_comment = '05238_view';

SELECT 'view: rows', count() FROM dst_view;
SELECT 'view: view rows', count() FROM mv;
SYSTEM FLUSH LOGS query_log;
SELECT 'view: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;
-- Reading still runs on all replicas: the arm above is not passing because parallel replicas were off.
SELECT 'view: distributed read', maxIf(ProfileEvents['ParallelReplicasUsedCount'] > 0, is_initial_query)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05238_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- 3) A replicated view target converges, but the write is refused on the presence of a dependent view
-- rather than on a proof that the whole reachable view graph converges.
DROP TABLE IF EXISTS dst_repl_view SYNC;
CREATE TABLE dst_repl_view (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_repl_view', 'r1') ORDER BY k;
CREATE MATERIALIZED VIEW mv_repl
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_repl_view_inner', 'r1') ORDER BY k
    AS SELECT k FROM dst_repl_view;

INSERT INTO dst_repl_view SELECT k FROM src SETTINGS log_comment = '05238_repl_view';

SELECT 'replicated view: rows', count() FROM dst_repl_view;
SELECT 'replicated view: view rows', count() FROM mv_repl;
SYSTEM FLUSH LOGS query_log;
SELECT 'replicated view: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_repl_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- 4) An `Alias` reports the target's engine capabilities, and a view can be attached to the alias itself,
-- where the check on the forwarded-to table does not see it. An alias target is refused either way.
DROP TABLE IF EXISTS al_view;
DROP TABLE IF EXISTS al_view_dest;
DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS dst_alias SYNC;
CREATE TABLE dst_alias (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_alias', 'r1') ORDER BY k;
CREATE TABLE al ENGINE = Alias(currentDatabase(), 'dst_alias');
CREATE TABLE al_view_dest (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW al_view TO al_view_dest AS SELECT k FROM al;

INSERT INTO al SELECT k FROM src SETTINGS log_comment = '05238_alias_view';

SELECT 'alias with a view on the alias: rows', count() FROM dst_alias;
SELECT 'alias with a view on the alias: view rows', count() FROM al_view_dest;
SYSTEM FLUSH LOGS query_log;
SELECT 'alias with a view on the alias: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_alias_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;
SELECT 'alias with a view on the alias: distributed read',
    maxIf(ProfileEvents['ParallelReplicasUsedCount'] > 0, is_initial_query)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05238_alias_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

DROP TABLE al_view;
DROP TABLE al_view_dest;

INSERT INTO al SELECT k FROM src SETTINGS log_comment = '05238_alias_no_view';

-- 30000 more rows through the alias: the fallback writes everything, it does not drop the INSERT.
SELECT 'alias without any view: rows', count() FROM dst_alias;
SYSTEM FLUSH LOGS query_log;
SELECT 'alias without any view: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_alias_no_view'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

-- 5) A sink that does not replicate at all (issue #121169). Every replica's sink resolves the same
-- destination file, so a distributed write would keep only one replica's share.
DROP TABLE IF EXISTS dst_file;
CREATE TABLE dst_file (k UInt64) ENGINE = File(TSV);

INSERT INTO dst_file SELECT k FROM src SETTINGS log_comment = '05238_file';

SELECT 'non-replicating sink: rows', count() FROM dst_file;
SYSTEM FLUSH LOGS query_log;
SELECT 'non-replicating sink: distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_file'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;
SELECT 'non-replicating sink: distributed read',
    maxIf(ProfileEvents['ParallelReplicasUsedCount'] > 0, is_initial_query)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '05238_file'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

DROP TABLE dst_file;
DROP TABLE al;
DROP TABLE dst_alias SYNC;
DROP TABLE mv_repl;
DROP TABLE dst_repl_view SYNC;
DROP TABLE mv;
DROP TABLE dst_view SYNC;
DROP TABLE dst_plain SYNC;
DROP TABLE src;
