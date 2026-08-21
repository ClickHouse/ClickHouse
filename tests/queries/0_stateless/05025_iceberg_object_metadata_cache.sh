#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

tmp_dir=$(mktemp -d "$USER_FILES_PATH/iceberg_object_cache.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS iceberg_object_cache;
    CREATE TABLE iceberg_object_cache (id UInt64)
        ENGINE=IcebergLocal('$tmp_dir/table', 'parquet');
    INSERT INTO iceberg_object_cache SETTINGS allow_insert_into_iceberg=1 VALUES (1), (2);"

$CLICKHOUSE_CLIENT --query_id=05025_first --query \
    "SELECT sum(id) FROM iceberg_object_cache SETTINGS use_query_condition_cache=0"
$CLICKHOUSE_CLIENT --query_id=05025_second --query \
    "SELECT sum(id) FROM iceberg_object_cache SETTINGS use_query_condition_cache=0"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT --query "
    SELECT
        ProfileEvents['IcebergObjectMetadataCacheMisses'] > 0,
        ProfileEvents['IcebergObjectMetadataCacheHits'] > 0
    FROM system.query_log
    WHERE type='QueryFinish' AND query_id IN ('05025_first', '05025_second')
    ORDER BY query_id"

$CLICKHOUSE_CLIENT --query "DROP TABLE iceberg_object_cache"
