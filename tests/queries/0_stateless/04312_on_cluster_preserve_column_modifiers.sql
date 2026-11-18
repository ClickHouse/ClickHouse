-- Tags: zookeeper, no-replicated-database
-- no-replicated-database: this test explicitly issues plain ON CLUSTER DDL on a regular database.

-- The initiator resolves the column types once and forwards the already-normalized query through the distributed DDL queue.
-- The worker replaying it must not re-apply data_type_default_nullable, otherwise the explicit NOT NULL would be wrapped into Nullable.
SET distributed_ddl_output_mode = 'throw';

DROP TABLE IF EXISTS t_on_cluster_not_null ON CLUSTER test_shard_localhost FORMAT Null;

SET data_type_default_nullable = 1;

CREATE TABLE t_on_cluster_not_null ON CLUSTER test_shard_localhost
(
    key Int64 NOT NULL,
    value String
)
ENGINE = MergeTree
ORDER BY tuple()
FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_on_cluster_not_null'
ORDER BY position;

DROP TABLE t_on_cluster_not_null ON CLUSTER test_shard_localhost FORMAT Null;


-- flatten_nested is normalized on the initiator the same way. Verify it is still applied exactly once
SET data_type_default_nullable = 0;
SET flatten_nested = 1;

CREATE TABLE t_on_cluster_nested ON CLUSTER test_shard_localhost
(
    id UInt64,
    n Nested(a UInt8, b String)
)
ENGINE = MergeTree
ORDER BY tuple()
FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_on_cluster_nested'
ORDER BY position;

DROP TABLE t_on_cluster_nested ON CLUSTER test_shard_localhost FORMAT Null;


-- ALTER TABLE ... ADD COLUMN and MODIFY COLUMN apply data_type_default_nullable on the initiator only, like CREATE.
SET data_type_default_nullable = 0, flatten_nested = 0;

CREATE TABLE t_on_cluster_alter ON CLUSTER test_shard_localhost (key Int64) ENGINE = MergeTree ORDER BY key FORMAT Null;

SET data_type_default_nullable = 1;

-- The oldest DDL entry format forwards no settings, so the worker sees only the type resolved on the initiator.
SET distributed_ddl_entry_format_version = 1;
ALTER TABLE t_on_cluster_alter ON CLUSTER test_shard_localhost ADD COLUMN added Int32, ADD COLUMN added_not_null Int32 NOT NULL FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_on_cluster_alter'
ORDER BY position;

ALTER TABLE t_on_cluster_alter ON CLUSTER test_shard_localhost MODIFY COLUMN added_not_null Int64 FORMAT Null;

SELECT type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_on_cluster_alter' AND name = 'added_not_null';

DROP TABLE t_on_cluster_alter ON CLUSTER test_shard_localhost FORMAT Null;
