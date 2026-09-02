#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ no-fasttest: the data lake engines are gated on build flags
# ^ no-msan: `delta-kernel-rs` is disabled under MSan

# Scanning `system.tables` must answer from metadata that is already loaded. When it instead fetched
# the metadata itself, every scan cost one failing remote fetch per data lake table whose storage had
# gone away, which kept a service in "Starting" for tens of minutes.
# https://github.com/ClickHouse/support-escalation/issues/8579

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Paths that do not exist, standing in for buckets that were deleted or are unreachable.
GONE="$CUR_DIR/data_delta_lake/${CLICKHOUSE_TEST_UNIQUE_NAME}_gone"

ATTACH_TABLES="
CREATE DATABASE d ENGINE = Memory;
ATTACH TABLE d.t1 (x UInt64) ENGINE = IcebergLocal('${GONE}_iceberg');
ATTACH TABLE d.t2 (x UInt64) ENGINE = DeltaLakeLocal('${GONE}_delta');
"

SCAN="SELECT count(), countIf(total_rows IS NULL), countIf(total_bytes IS NULL) FROM system.tables WHERE database = 'd';"

# Both tables are listed, with unknown totals rather than a failure.
$CLICKHOUSE_LOCAL --multiquery "$ATTACH_TABLES $SCAN" 2>/dev/null

# No scan reads the storage, however many times the listing is repeated.
$CLICKHOUSE_LOCAL --send_logs_level=warning --multiquery "$ATTACH_TABLES $SCAN $SCAN $SCAN" 2>&1 \
    | grep -c -F 'StorageSystemTables' || true
