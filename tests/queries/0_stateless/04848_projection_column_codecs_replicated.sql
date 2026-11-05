-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codecs_r1;
DROP TABLE IF EXISTS t_projection_codecs_r2;
DROP TABLE IF EXISTS t_projection_codecs_r3;
DROP TABLE IF EXISTS t_projection_codecs_r4;
DROP TABLE IF EXISTS t_projection_codecs_denied;
DROP TABLE IF EXISTS t_projection_codecs_gate;
DROP TABLE IF EXISTS t_projection_codecs_copy;
DROP TABLE IF EXISTS t_projection_codecs_cluster ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS t_projection_codecs_bad;

SET distributed_ddl_output_mode = 'throw';

-- Shared metadata and DDL queues fail closed until an operator confirms every consumer is upgraded.
CREATE TABLE t_projection_codecs_denied
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs_denied', 'r1') ORDER BY k; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codecs_denied';

CREATE TABLE t_projection_codecs_gate (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs_gate', 'r1') ORDER BY k;
ALTER TABLE t_projection_codecs_gate ADD PROJECTION p
    (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k); -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codecs_gate';
DROP TABLE t_projection_codecs_gate;

CREATE TABLE t_projection_codecs_cluster ON CLUSTER test_shard_localhost
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codecs_cluster';

SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;
SET distributed_ddl_entry_format_version = 1;
CREATE TABLE t_projection_codecs_cluster ON CLUSTER test_shard_localhost
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }
SET distributed_ddl_entry_format_version = 5;

CREATE TABLE t_projection_codecs_r1
(
    k UInt64,
    v UInt64,
    PROJECTION p (v CODEC(Delta, ZSTD)) AS (SELECT k, v ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs', 'r1') ORDER BY k;

-- Replicas must use the same codec declaration for each projection column.
CREATE TABLE t_projection_codecs_r2
(
    k UInt64,
    v UInt64,
    PROJECTION p (v CODEC(Delta, ZSTD)) AS (SELECT k, v ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs', 'r2') ORDER BY k;

-- Equivalent writer codecs with different declarations must not pass metadata comparison.
CREATE TABLE t_projection_codecs_r3
    (k UInt64, v UInt64, PROJECTION p (v CODEC(Delta, ZSTD(1))) AS (SELECT k, v ORDER BY k))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs', 'r3') ORDER BY k; -- { serverError METADATA_MISMATCH }

-- The omitted Delta width must retain its intent across a later output-type change.
CREATE TABLE t_projection_codecs_r3
    (k UInt64, v UInt64, PROJECTION p (v CODEC(Delta(8), ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs', 'r3') ORDER BY k; -- { serverError METADATA_MISMATCH }

-- A CREATE AS copy gets the same gate after its source projections are materialized.
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 0;
DETACH TABLE t_projection_codecs_r2;
ATTACH TABLE t_projection_codecs_r2;
CREATE TABLE t_projection_codecs_copy AS t_projection_codecs_r1
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs_copy', 'r1') ORDER BY k; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codecs_copy';
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;

INSERT INTO t_projection_codecs_r1 SELECT number, number FROM numbers(1000);
SYSTEM SYNC REPLICA t_projection_codecs_r2;
SELECT sum(v) = 499500 FROM t_projection_codecs_r2;

-- The replica applies both projection metadata and a changed output type.
ALTER TABLE t_projection_codecs_r1 ADD PROJECTION q
    (v CODEC(DoubleDelta, ZSTD)) AS (SELECT k, v ORDER BY k) SETTINGS alter_sync = 2;
SYSTEM SYNC REPLICA t_projection_codecs_r2;
CREATE TABLE t_projection_codecs_r4
    (k UInt64, v UInt64,
     PROJECTION p (v CODEC(Delta, ZSTD)) AS (SELECT k, v ORDER BY k),
     PROJECTION q (v CODEC(DoubleDelta(8), ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04848_projection_codecs', 'r4') ORDER BY k; -- { serverError METADATA_MISMATCH }
SELECT position(create_table_query, 'CODEC(Delta, ZSTD') > 0
    AND position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_r2';
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name IN ('t_projection_codecs_r3', 't_projection_codecs_r4');

ALTER TABLE t_projection_codecs_r1 MODIFY COLUMN v UInt32 SETTINGS alter_sync = 2, mutations_sync = 2;
SYSTEM SYNC REPLICA t_projection_codecs_r2;
SELECT type = 'UInt32' FROM system.columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_r2' AND name = 'v';
SELECT sum(v) = 499500 FROM t_projection_codecs_r2;
SELECT position(create_table_query, 'CODEC(Delta, ZSTD') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_r2';
INSERT INTO t_projection_codecs_r1 SELECT number + 1000, number + 1000 FROM numbers(1000);
SYSTEM SYNC REPLICA t_projection_codecs_r2;
SELECT sum(v) = 1999000 FROM t_projection_codecs_r2;
SELECT countDistinct(name) = 2 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_r2'
    AND active AND column = 'v' AND name IN ('p', 'q');

-- ON CLUSTER sends the declaration to a DDL worker.
CREATE TABLE t_projection_codecs_cluster ON CLUSTER test_shard_localhost
(
    k UInt64,
    v UInt64,
    PROJECTION p (v CODEC(Delta, ZSTD)) AS (SELECT k, v ORDER BY k)
)
ENGINE = MergeTree ORDER BY k FORMAT Null;
ALTER TABLE t_projection_codecs_cluster ON CLUSTER test_shard_localhost ADD PROJECTION q
    (v CODEC(DoubleDelta, ZSTD)) AS (SELECT k, v ORDER BY k) FORMAT Null;
INSERT INTO t_projection_codecs_cluster SELECT number, number FROM numbers(1000);
SELECT position(create_table_query, 'CODEC(Delta, ZSTD)') > 0
    AND position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_cluster';
SELECT sum(v) = 499500 FROM t_projection_codecs_cluster;

CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (x UInt64 CODEC(NONE)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (x CODEC(Default)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_projection_codecs_bad
    (x Float64, PROJECTION p (x CODEC(SZ3)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (x CODEC(NoSuchCodec)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE t_projection_codecs_bad
    (obj Tuple(x UInt64), PROJECTION p (`obj.x` CODEC(ZSTD)) AS (SELECT obj.x ORDER BY obj.x))
ENGINE = MergeTree ORDER BY tuple(); -- { serverError NOT_IMPLEMENTED }

-- A session opt-in must not change metadata admission on other replicas.
SET allow_suspicious_codecs = 1;
CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }
SET allow_suspicious_codecs = 0;

CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (x CODEC(NONE), x CODEC(ZSTD)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError DUPLICATE_COLUMN }

CREATE TABLE t_projection_codecs_bad
    (x UInt64, PROJECTION p (y CODEC(NONE)) AS (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x; -- { serverError THERE_IS_NO_COLUMN }

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codecs_bad';

DROP TABLE t_projection_codecs_cluster ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE t_projection_codecs_r2;
DROP TABLE t_projection_codecs_r1;
