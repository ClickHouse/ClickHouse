#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test Iceberg v3 nanosecond timestamp support (timestamp_ns, timestamptz_ns)
# These types map to DateTime64(9) and DateTime64(9, 'UTC') respectively

# If USER_FILES_PATH doesn't exist (local dev), query the server for the actual path
if [ ! -d "${USER_FILES_PATH}" ]; then
    USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
fi

# Setup test directory using standard pattern from other tests
unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
TEST_DIR=${USER_FILES_PATH}/${unique_name}

function cleanup()
{
    rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

# Test 1: Verify setting exists and defaults to true
$CLICKHOUSE_CLIENT --query "SELECT value FROM system.settings WHERE name = 'iceberg_allow_nanosecond_timestamps'" | grep -q "1" && echo "Setting exists and defaults to true: OK" || echo "Setting exists and defaults to true: FAILED"

# Test 2: Verify setting can be changed
$CLICKHOUSE_CLIENT --query "SET iceberg_allow_nanosecond_timestamps = 0; SELECT getSetting('iceberg_allow_nanosecond_timestamps')" | grep -q "false" && echo "Setting can be disabled: OK" || echo "Setting can be disabled: FAILED"

# Test 3: Create minimal synthetic Iceberg v3 table with timestamp_ns
# Note: We create the metadata structure manually since Spark doesn't support these types yet
mkdir -p "${TEST_DIR}/metadata"
mkdir -p "${TEST_DIR}/data"

# Create v3 metadata with timestamp_ns and timestamptz_ns types
cat > "${TEST_DIR}/metadata/v1.metadata.json" << 'EOF'
{
  "format-version": 3,
  "table-uuid": "12345678-1234-1234-1234-123456789012",
  "location": "test_location",
  "last-updated-ms": 1700000000000,
  "last-column-id": 3,
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "ts_nano", "required": false, "type": "timestamp_ns"},
      {"id": 2, "name": "ts_nano_tz", "required": false, "type": "timestamptz_ns"},
      {"id": 3, "name": "id", "required": false, "type": "int"}
    ]
  },
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "ts_nano", "required": false, "type": "timestamp_ns"},
        {"id": 2, "name": "ts_nano_tz", "required": false, "type": "timestamptz_ns"},
        {"id": 3, "name": "id", "required": false, "type": "int"}
      ]
    }
  ],
  "partition-spec": [],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "properties": {},
  "current-snapshot-id": -1,
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": []
}
EOF

# Test 4: DESCRIBE icebergLocal - verify type mappings
echo "Type mappings (DESCRIBE icebergLocal):"
$CLICKHOUSE_CLIENT --query "DESCRIBE TABLE icebergLocal('${TEST_DIR}/', 'Parquet') SETTINGS iceberg_allow_nanosecond_timestamps = 1" | cut -f 1,2 | grep -E '^(id|ts_nano)' | sort

# Test 5: Verify error when setting is disabled and metadata contains nanosecond types
$CLICKHOUSE_CLIENT -m --query "
SET iceberg_allow_nanosecond_timestamps = 0;
SELECT * FROM icebergLocal('${TEST_DIR}/', 'Parquet') LIMIT 0;
" 2>&1 | grep -q "nanosecond timestamp types.*are not enabled" && echo "Error detection works: OK" || echo "Error detection works: FAILED"
