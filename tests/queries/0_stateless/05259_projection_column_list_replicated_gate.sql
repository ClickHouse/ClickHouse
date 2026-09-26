-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree

-- Suppress per-replica DDL status rows; the mode still drains the status pipeline.
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_projection_column_list_gate;
DROP TABLE IF EXISTS t_projection_column_list_gate_create;
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

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
