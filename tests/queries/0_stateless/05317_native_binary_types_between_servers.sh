#!/usr/bin/env bash
# Server-to-server queries and a client's secondary query work with the binary type encoding of the Native format
# enabled, and a shard still applies these settings to the formats it reads itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLIENT_BINARY_TYPES="$CLICKHOUSE_CLIENT --output_format_native_encode_types_in_binary_format 1 --input_format_native_decode_types_in_binary_format 1"

$CLIENT_BINARY_TYPES <<'EOF'
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
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0,
    automatic_parallel_replicas_mode = 0, enable_parallel_blocks_marshalling = 1, log_comment = '05317_parallel_replicas';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05317_parallel_replicas'
    AND type = 'QueryFinish' AND query_id = initial_query_id
SETTINGS enable_parallel_replicas = 0;

SELECT 'remote insert';
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_dst) VALUES (7);
SELECT x FROM t_dst;

SELECT 'file read on a shard';
INSERT INTO FUNCTION file(currentDatabase() || '_05317.native', Native) SELECT 5::UInt64 AS x SETTINGS engine_file_truncate_on_insert = 1;
SELECT x FROM remote('127.0.0.2', file(currentDatabase() || '_05317.native', Native));
EOF

echo 'secondary query from a client'
$CLIENT_BINARY_TYPES --query_kind secondary_query -q "INSERT INTO t_dst VALUES (8)"
$CLIENT_BINARY_TYPES --query_kind secondary_query -q "SELECT d, dynamicType(d) FROM t_src ORDER BY toString(d) SETTINGS enable_parallel_replicas = 0"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t_dst ORDER BY x"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_src; DROP TABLE t_dst"
