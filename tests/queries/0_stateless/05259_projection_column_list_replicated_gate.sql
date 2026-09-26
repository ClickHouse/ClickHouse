-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree

-- Suppress per-replica DDL status rows; the mode still drains the status pipeline.
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_projection_column_list_gate;
DROP TABLE IF EXISTS t_projection_column_list_gate_create;
DROP TABLE IF EXISTS t_projection_column_list_cluster_create ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS t_projection_column_list_cluster_alter ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS t_projection_column_list_cluster_copy ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS t_projection_column_list_cluster_inherited ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS t_projection_column_list_cluster_memory ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS default.t_05259_projection_column_list_source;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

-- A local MergeTree may use the syntax without the compatibility override.
CREATE TABLE t_projection_column_list_gate_create
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x;
DROP TABLE t_projection_column_list_gate_create;

-- A replicated table must reject the new metadata syntax by default.
CREATE TABLE t_projection_column_list_gate_create
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_gate_create', 'r1')
    ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_column_list_gate_create';

CREATE TABLE t_projection_column_list_gate (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_gate', 'r1') ORDER BY x;
ALTER TABLE t_projection_column_list_gate
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_column_list_gate';

-- A duplicate installs no declaration, but its syntax would still enter a DDL log.
ALTER TABLE t_projection_column_list_gate ADD PROJECTION p (SELECT x ORDER BY x);
ALTER TABLE t_projection_column_list_gate
    ADD PROJECTION IF NOT EXISTS p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_column_list_gate';
ALTER TABLE t_projection_column_list_gate
    DROP PROJECTION p,
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_column_list_gate';

SET allow_projection_column_list_in_replicated_metadata = 1;
ALTER TABLE t_projection_column_list_gate
    ADD PROJECTION IF NOT EXISTS p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x);
ALTER TABLE t_projection_column_list_gate
    DROP PROJECTION p,
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x);
SELECT count() FROM system.projections WHERE database = currentDatabase() AND table = 't_projection_column_list_gate';
DROP TABLE t_projection_column_list_gate;

-- Format 1 sends no initiator settings. A rejected query must fail before it is enqueued;
-- timeout 0 would otherwise return success without waiting for the worker's error.
SET distributed_ddl_entry_format_version = 1;
SET distributed_ddl_task_timeout = 0;
SET allow_projection_column_list_in_replicated_metadata = 0;
-- The old-format worker's current database is `default`. Qualify the source on both legs.
CREATE TABLE default.t_05259_projection_column_list_source
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_projection_column_list_cluster_create ON CLUSTER test_shard_localhost
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_column_list_cluster_create';

CREATE TABLE t_projection_column_list_cluster_alter (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_alter', 'r1') ORDER BY x;
ALTER TABLE t_projection_column_list_cluster_alter ON CLUSTER test_shard_localhost
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_alter';

-- The syntax can also arrive indirectly: old-format workers expand AS <source> after enqueueing.
CREATE TABLE t_projection_column_list_cluster_copy ON CLUSTER test_shard_localhost
    AS default.t_05259_projection_column_list_source
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_copy', 'r1')
    ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_column_list_cluster_copy';
CREATE TABLE t_projection_column_list_cluster_inherited ON CLUSTER test_shard_localhost
    AS default.t_05259_projection_column_list_source; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_column_list_cluster_inherited';

-- The worker must accept these entries with its own default-off setting after the initiator opts in.
-- Use replicated targets so both worker paths would hit the compatibility gate without the replay guard.
SET distributed_ddl_task_timeout = 180;
SET distributed_ddl_output_mode = 'throw';
-- A destination that cannot store projections is safe, even when the source has a column list.
CREATE TABLE t_projection_column_list_cluster_memory ON CLUSTER test_shard_localhost
    AS default.t_05259_projection_column_list_source ENGINE = Memory FORMAT Null;
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_memory';
SET allow_projection_column_list_in_replicated_metadata = 1;
CREATE TABLE t_projection_column_list_cluster_create ON CLUSTER test_shard_localhost
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_create', 'r1')
    ORDER BY x FORMAT Null;
ALTER TABLE t_projection_column_list_cluster_alter ON CLUSTER test_shard_localhost
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x) FORMAT Null;
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_create';
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_alter';
CREATE TABLE t_projection_column_list_cluster_copy ON CLUSTER test_shard_localhost
    AS default.t_05259_projection_column_list_source
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_projection_column_list_cluster_copy', 'r1')
    ORDER BY x FORMAT Null;
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_copy';
CREATE TABLE t_projection_column_list_cluster_inherited ON CLUSTER test_shard_localhost
    AS default.t_05259_projection_column_list_source FORMAT Null;
SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_column_list_cluster_inherited';
DROP TABLE t_projection_column_list_cluster_create ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE t_projection_column_list_cluster_alter ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE t_projection_column_list_cluster_copy ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE t_projection_column_list_cluster_inherited ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE t_projection_column_list_cluster_memory ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE default.t_05259_projection_column_list_source;
SET distributed_ddl_output_mode = 'none';
SET distributed_ddl_entry_format_version = 5;
SET allow_projection_column_list_in_replicated_metadata = 0;

-- A Replicated database needs the same gate even for an ordinary MergeTree table.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
    ENGINE = Replicated('/clickhouse/databases/{database}/projection_column_list_gate', 's1', 'r1');
SET allow_projection_column_list_in_replicated_metadata = 0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_create
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_create';

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_alter (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_alter
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_alter';

SET allow_projection_column_list_in_replicated_metadata = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_create
    (x UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
    ENGINE = MergeTree ORDER BY x;
SELECT count() FROM system.projections WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_create';
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_alter
    ADD PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x);
SELECT count() FROM system.projections WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_alter';

SET allow_projection_column_list_in_replicated_metadata = 0;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_alter
    MODIFY PROJECTION p (x CODEC(ZSTD)) AS (SELECT x ORDER BY x)
    WITH SETTINGS (index_granularity = 8192); -- { serverError SUPPORT_IS_DISABLED }

-- RESTORE supplies a fresh definition even though it uses SECONDARY_CREATE loading mode.
-- Refuse to publish that definition into the Replicated database log without the override.
USE {CLICKHOUSE_DATABASE_1:Identifier};
BACKUP TABLE t_create
    TO Memory('05259_projection_column_list_restore') FORMAT Null;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_create SYNC;
RESTORE TABLE t_create
    FROM Memory('05259_projection_column_list_restore') FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 't_create';

SET allow_projection_column_list_in_replicated_metadata = 1;
RESTORE TABLE t_create
    FROM Memory('05259_projection_column_list_restore') FORMAT Null;
SELECT count() FROM system.projections
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't_create';

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
