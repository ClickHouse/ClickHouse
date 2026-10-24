-- Tags: no-replicated-database
--       no-replicated-database: `DETACH DATABASE` / `ATTACH DATABASE` of an `Atomic` database with the
--       `lazy_load_tables` setting.

-- A table of a database with `lazy_load_tables = 1` is a `StorageTableProxy` stand-in until its first
-- access, and `StorageProxy` forwards `supportsReplication()` but not `isMergeTree()`. The write side of
-- `parallel_distributed_insert_select = 2` classifies its target through that stand-in, so a replicated
-- target keeps the distributed write however its database happens to be attached.

SET enable_analyzer = 1; -- parallel distributed insert select for replicated tables works only with analyzer
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_distributed_insert_select = 2;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

-- The source stays in an eagerly loaded database: a lazily proxied source is not eligible for parallel
-- replicas on the read side either, which would mask the target-side decision under test.
DROP TABLE IF EXISTS src;
CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO src SELECT number FROM numbers(30000);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dst (k UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05238_lazy_dst', 'r1') ORDER BY k;

-- Re-attach so the target is an unloaded proxy when the INSERT below is planned.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Prove the fixture: the target is still a stand-in. The `system.tables` filter is spelled with
-- `currentDatabase()` because the style check only recognizes that form, and `USE` does not load the
-- lazy tables.
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 'target engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 'dst';
USE {CLICKHOUSE_DATABASE:Identifier};

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.dst SELECT k FROM src SETTINGS log_comment = '05238_lazy';

SELECT 'rows', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.dst;
SYSTEM FLUSH LOGS query_log;
SELECT 'distributed write', count() > 1 FROM system.query_log
WHERE type = 'QueryStart' AND query_kind = 'Insert' AND log_comment = '05238_lazy'
    AND (current_database = currentDatabase() OR has(databases, currentDatabase()))
    AND event_date >= yesterday() AND event_time >= now() - 600;

DROP TABLE src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
