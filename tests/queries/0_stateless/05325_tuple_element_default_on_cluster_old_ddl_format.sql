-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed for Replicated database.

-- Tuple-element `DEFAULT` expressions must be pulled up to the column level before
-- a `CREATE` query is dispatched in the old distributed DDL entry format.

SET distributed_ddl_entry_format_version = 2;
DROP TABLE IF EXISTS t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
CREATE TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost
(
    id UInt8,
    c Tuple(a UInt8, s String DEFAULT 'Hello')
)
ENGINE = MergeTree ORDER BY id FORMAT Null;
SELECT type, default_kind, default_expression
FROM system.columns
WHERE database = currentDatabase() AND table = 't_default_in_tuple_cluster' AND name = 'c';
DROP TABLE t_default_in_tuple_cluster ON CLUSTER test_shard_localhost FORMAT Null;
