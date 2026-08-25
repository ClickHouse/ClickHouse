#!/usr/bin/env bash
# Test the snapshot-scoped Iceberg manifest partition-candidate cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

# Create a simple Iceberg table via IcebergLocal (file-based, no REST catalog)
TMP_DIR=$(mktemp -d "$USER_FILES_PATH/iceberg_prune_test.XXXXXX")
TABLE_PATH="$TMP_DIR/iceberg_prune_test"
mkdir -p "$TABLE_PATH"
trap 'rm -rf "$TMP_DIR"' EXIT

# Unique query ids per run: query_log persists in the server, and the test
# harness may retry on the same server, so fixed ids would match stale rows.
QID_PREFIX="q_prune_${$}_"

# Use clickhouse to create Iceberg table with partitioning, then query with same filter twice
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS iceberg_prune_test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE iceberg_prune_test (id Int32, part Int32) ENGINE=IcebergLocal('$TABLE_PATH', 'parquet') PARTITION BY part"

# Insert some data with partitions 0..3
$CLICKHOUSE_CLIENT -q "INSERT INTO iceberg_prune_test SETTINGS allow_insert_into_iceberg=1 VALUES (1, 0), (2, 1), (3, 2), (4, 3), (5, 1)"

# First query with filter that prunes to part=1 (should miss prune cache)
$CLICKHOUSE_CLIENT -q "SELECT * FROM iceberg_prune_test WHERE part = 1 SETTINGS use_iceberg_partition_pruning=1" --query_id="${QID_PREFIX}1" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['IcebergManifestPruneCacheMisses'] > 0 AS miss1, ProfileEvents['IcebergPartitionPrunedFiles'] > 0 AS pruned1 FROM system.query_log WHERE query_id='${QID_PREFIX}1' AND type='QueryFinish'"

# A different point literal in the same partition should jump directly to
# the cached candidate rows.
$CLICKHOUSE_CLIENT -q "SELECT * FROM iceberg_prune_test WHERE part = 1 AND id = 5 SETTINGS use_iceberg_partition_pruning=1" --query_id="${QID_PREFIX}2" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['IcebergManifestPruneCacheHits'] > 0 AS hit2 FROM system.query_log WHERE query_id='${QID_PREFIX}2' AND type='QueryFinish'"

# A new snapshot must not reuse the old candidate vector; its newly added
# file has to be visible immediately.
$CLICKHOUSE_CLIENT -q "INSERT INTO iceberg_prune_test SETTINGS allow_insert_into_iceberg=1 VALUES (6, 1)"
$CLICKHOUSE_CLIENT -q "SELECT id FROM iceberg_prune_test WHERE part = 1 AND id = 6 SETTINGS use_iceberg_partition_pruning=1" --query_id="${QID_PREFIX}3"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['IcebergManifestPruneCacheMisses'] > 0 AS miss3 FROM system.query_log WHERE query_id='${QID_PREFIX}3' AND type='QueryFinish'"

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR ICEBERG MANIFEST PRUNE CACHE"
echo "CLEAR OK"
$CLICKHOUSE_CLIENT -q "SELECT * FROM iceberg_prune_test WHERE part = 1 SETTINGS use_iceberg_partition_pruning=1" --query_id="${QID_PREFIX}4" > /dev/null
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['IcebergManifestPruneCacheMisses'] > 0 AS miss4 FROM system.query_log WHERE query_id='${QID_PREFIX}4' AND type='QueryFinish'"

$CLICKHOUSE_CLIENT -q "DROP TABLE iceberg_prune_test"
echo "OK"
