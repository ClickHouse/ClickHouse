-- Queries that read or write through a server-to-server connection work with the binary type encoding
-- of the Native format enabled, and a shard still applies these settings to the formats it reads itself.

SET output_format_native_encode_types_in_binary_format = 1, input_format_native_decode_types_in_binary_format = 1;

DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_src (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_src VALUES (42::UInt64), ('str');
CREATE TABLE t_dst (x UInt64) ENGINE = MergeTree ORDER BY x;

SELECT 'remote';
SELECT d, dynamicType(d) FROM remote('127.0.0.2', currentDatabase(), t_src) ORDER BY toString(d);

SELECT 'parallel replicas';
SELECT d, dynamicType(d) FROM t_src ORDER BY toString(d)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

SELECT 'remote insert';
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_dst) VALUES (7);
SELECT x FROM t_dst;

SELECT 'file read on a shard';
INSERT INTO FUNCTION file(currentDatabase() || '_05317.native', Native) SELECT 5::UInt64 AS x SETTINGS engine_file_truncate_on_insert = 1;
SELECT x FROM remote('127.0.0.2', file(currentDatabase() || '_05317.native', Native));

DROP TABLE t_src;
DROP TABLE t_dst;
