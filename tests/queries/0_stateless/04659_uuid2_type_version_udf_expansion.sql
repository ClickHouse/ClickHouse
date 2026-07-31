-- With `uuid_type_version = 2`, a bare `UUID` introduced by an expanded SQL UDF body (`CAST(x, 'UUID')` and
-- the like) must be materialized as well. The `ALTER` path expands UDFs after parsing, and the legacy
-- `CREATE ... ON CLUSTER` path (`distributed_ddl_entry_format_version < 3`) enqueues the query before the
-- normal on-worker UDF expansion, so both have to order UDF expansion before the materialization pass.

DROP TABLE IF EXISTS t_udf_uuid2_default;
DROP TABLE IF EXISTS t_udf_uuid2_mutation;
DROP TABLE IF EXISTS t_udf_uuid2_mv_source;
DROP TABLE IF EXISTS t_udf_uuid2_mv;
DROP FUNCTION IF EXISTS udf_04659_to_uuid;

CREATE FUNCTION udf_04659_to_uuid AS s -> CAST(s, 'UUID');

SET uuid_type_version = 2;

SELECT 'a UDF-introduced bare UUID in an ALTER-added default expression';
CREATE TABLE t_udf_uuid2_default (k UInt8, s String) ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_udf_uuid2_default ADD COLUMN x UUID DEFAULT udf_04659_to_uuid(s);
SELECT name, type, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_udf_uuid2_default' AND name = 'x';

SELECT 'a UDF-introduced bare UUID in a mutation expression';
CREATE TABLE t_udf_uuid2_mutation (k UInt8, s String, x UUID) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_udf_uuid2_mutation VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '00000000-0000-0000-0000-000000000000');
SET mutations_sync = 2;
ALTER TABLE t_udf_uuid2_mutation UPDATE x = udf_04659_to_uuid(s) WHERE k = 1;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_udf_uuid2_mutation';
SELECT x, toTypeName(x) FROM t_udf_uuid2_mutation;

SELECT 'a UDF-introduced bare UUID in ALTER MODIFY QUERY';
CREATE TABLE t_udf_uuid2_mv_source (k UInt8, s String) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW t_udf_uuid2_mv ENGINE = MergeTree ORDER BY k AS SELECT k, CAST(s, 'UUID') AS x FROM t_udf_uuid2_mv_source;
ALTER TABLE t_udf_uuid2_mv MODIFY QUERY SELECT k, udf_04659_to_uuid(s) AS x FROM t_udf_uuid2_mv_source;
SELECT replaceAll(as_select, currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_udf_uuid2_mv';

DROP TABLE t_udf_uuid2_default;
DROP TABLE t_udf_uuid2_mutation;
DROP TABLE t_udf_uuid2_mv;
DROP TABLE t_udf_uuid2_mv_source;

SELECT 'a UDF-introduced bare UUID in a legacy ON CLUSTER CREATE';
SET distributed_ddl_output_mode = 'none';
SET distributed_ddl_entry_format_version = 2;
DROP TABLE IF EXISTS t_udf_uuid2_cluster ON CLUSTER test_shard_localhost SYNC;
CREATE TABLE t_udf_uuid2_cluster ON CLUSTER test_shard_localhost (k UInt8, s String, x UUID DEFAULT udf_04659_to_uuid(s)) ENGINE = MergeTree ORDER BY k;
SELECT name, type, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_udf_uuid2_cluster' AND name = 'x';
DROP TABLE t_udf_uuid2_cluster ON CLUSTER test_shard_localhost SYNC;

SELECT 'the historical type is unchanged under uuid_type_version = 1';
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_udf_uuid1_cluster ON CLUSTER test_shard_localhost SYNC;
CREATE TABLE t_udf_uuid1_cluster ON CLUSTER test_shard_localhost (k UInt8, s String, x UUID DEFAULT udf_04659_to_uuid(s)) ENGINE = MergeTree ORDER BY k;
SELECT name, type, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_udf_uuid1_cluster' AND name = 'x';
DROP TABLE t_udf_uuid1_cluster ON CLUSTER test_shard_localhost SYNC;
SET distributed_ddl_entry_format_version = DEFAULT;
SET distributed_ddl_output_mode = DEFAULT;

DROP FUNCTION udf_04659_to_uuid;
