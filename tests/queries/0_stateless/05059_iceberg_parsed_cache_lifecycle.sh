#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

TABLE_PATH="$USER_FILES_PATH/$CLICKHOUSE_TEST_UNIQUE_NAME"
mkdir -p "$TABLE_PATH"
trap 'rm -rf "$TABLE_PATH"' EXIT
READER_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
DISABLED_UUID=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# ATTACH with a full definition logs a server-side warning; silence log delivery
# for those statements so clickhouse-test does not fail on stderr.
$CLICKHOUSE_CLIENT --multiquery <<EOF
CREATE TABLE parsed_cache_source (id UInt64) ENGINE=IcebergLocal('$TABLE_PATH');
INSERT INTO parsed_cache_source SETTINGS allow_insert_into_iceberg=1 VALUES (1), (2), (3);
SELECT sum(id) FROM parsed_cache_source;
ATTACH TABLE parsed_cache_reader UUID '$READER_UUID' (id UInt64) ENGINE=IcebergLocal('$TABLE_PATH') SETTINGS send_logs_level='fatal';
SELECT sum(id) FROM parsed_cache_reader;
EOF

# Construct a different storage while caching is disabled, rather than changing
# the setting on a storage that already owns a cache pointer.
$CLICKHOUSE_CLIENT --use_iceberg_metadata_files_cache=0 --multiquery <<EOF
ATTACH TABLE parsed_cache_disabled UUID '$DISABLED_UUID' (id UInt64) ENGINE=IcebergLocal('$TABLE_PATH') SETTINGS send_logs_level='fatal';
SELECT sum(id) FROM parsed_cache_disabled;
DROP TABLE parsed_cache_disabled;
EOF

$CLICKHOUSE_CLIENT --multiquery <<EOF
INSERT INTO parsed_cache_source SETTINGS allow_insert_into_iceberg=1 VALUES (4);
SELECT sum(id) FROM parsed_cache_reader SETTINGS iceberg_metadata_staleness_ms=0;
DROP TABLE parsed_cache_reader;
DROP TABLE parsed_cache_source;
EOF
