-- Tags: no-replicated-database
-- no-replicated-database: ON CLUSTER DDL inside a Replicated database takes a different path and changes the output.

-- The guard runs on the initiator before dispatch with the default entry format, and on the hosts with older
-- formats. Format 2 carries the initiator's settings to the hosts; format 1 carries none, so the hosts apply
-- their own defaults.

SET distributed_ddl_output_mode = 'none';
SET enable_materialized_cte = 1;
SET force_materialized_cte = 1;

SELECT 'default entry format: rejected on the initiator';
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'entry format 2: rejected on the host';
SET distributed_ddl_entry_format_version = 2;
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'entry format 1: rejected on the host';
SET distributed_ddl_entry_format_version = 1;
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'guard off, default entry format: accepted';
SET force_materialized_cte = 0;
SET distributed_ddl_entry_format_version = DEFAULT;
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b;
SELECT * FROM v_05143;
DROP TABLE v_05143 ON CLUSTER test_shard_localhost;

SELECT 'guard off, entry format 2: the host receives the setting and accepts';
SET distributed_ddl_entry_format_version = 2;
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b;
SELECT * FROM v_05143;
DROP TABLE v_05143 ON CLUSTER test_shard_localhost;

SELECT 'guard off, entry format 1: the host uses its own defaults and rejects';
SET distributed_ddl_entry_format_version = 1;
CREATE VIEW v_05143 ON CLUSTER test_shard_localhost AS WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'MODIFY QUERY on cluster: rejected on the host, accepted with the guard off';
SET distributed_ddl_entry_format_version = DEFAULT;
SET force_materialized_cte = 1;
-- A materialized view must read from a table, not a table function.
CREATE TABLE src_05143 ON CLUSTER test_shard_localhost (x UInt64) ENGINE = Memory;
INSERT INTO src_05143 SELECT number FROM numbers(3);
CREATE TABLE dst_05143 ON CLUSTER test_shard_localhost (n UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_05143 ON CLUSTER test_shard_localhost TO dst_05143 AS SELECT count() AS n FROM src_05143;
ALTER TABLE mv_05143 ON CLUSTER test_shard_localhost MODIFY QUERY WITH c AS MATERIALIZED (SELECT x FROM src_05143) SELECT count() AS n FROM c AS a, c AS b; -- { serverError SUPPORT_IS_DISABLED }
SET force_materialized_cte = 0;
ALTER TABLE mv_05143 ON CLUSTER test_shard_localhost MODIFY QUERY WITH c AS MATERIALIZED (SELECT x FROM src_05143) SELECT count() AS n FROM c AS a, c AS b;
DROP TABLE mv_05143 ON CLUSTER test_shard_localhost;
DROP TABLE dst_05143 ON CLUSTER test_shard_localhost;
DROP TABLE src_05143 ON CLUSTER test_shard_localhost;
