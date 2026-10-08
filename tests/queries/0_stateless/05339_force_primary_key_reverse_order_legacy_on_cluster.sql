-- Tags: distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

-- With `distributed_ddl_entry_format_version` below 3 the initiator ships the query text as written,
-- so `force_primary_key_reverse_order` must be applied before the query is enqueued.

SET distributed_ddl_output_mode = 'none';
SET force_primary_key_reverse_order = 1;

DROP TABLE IF EXISTS t_reverse_legacy_1;
DROP TABLE IF EXISTS t_reverse_legacy_2;
DROP TABLE IF EXISTS t_reverse_current;

SET distributed_ddl_entry_format_version = 1;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_reverse_legacy_1 ON CLUSTER test_shard_localhost (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);

SET distributed_ddl_entry_format_version = 2;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_reverse_legacy_2 ON CLUSTER test_shard_localhost (a UInt32) ENGINE = MergeTree ORDER BY a;

SET distributed_ddl_entry_format_version = 6;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_reverse_current ON CLUSTER test_shard_localhost (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);

-- `sorting_key` in `system.tables` does not show the direction, so it is taken from the definition.
SELECT name, extract(create_table_query, 'ORDER BY (.+?) SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_reverse_%' ORDER BY name;

DROP TABLE t_reverse_legacy_1;
DROP TABLE t_reverse_legacy_2;
DROP TABLE t_reverse_current;
