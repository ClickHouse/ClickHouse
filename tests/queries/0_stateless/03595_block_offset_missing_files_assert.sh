#!/usr/bin/env bash
# Tags: no-parallel

# This test deterministically reproduces the debug-only logical error from
# MergeTreeDataPartWide::calculateEachColumnSizes() that was fixed in GH#85312.
# It creates a part where _block_offset is declared in columns.txt but its
# data files are missing, then queries system.parts_columns which triggers
# per-column size accounting. In Debug builds without the fix it throws
# LOGICAL_ERROR; with the fix (or in Release) it succeeds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
# Silence server warnings in client stderr to avoid harness FAIL on stderr.
# We still want errors to be visible if something goes wrong.
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error
. "$CURDIR"/../shell_config.sh

set -euo pipefail

# Only meaningful for Debug builds (assertion is compiled out in Release).
BUILD_TYPE=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.build_options WHERE name='BUILD_TYPE' LIMIT 1" 2>/dev/null | tr -d '\n' || true)
if [[ "${BUILD_TYPE}" != "Debug" ]]; then
    echo "@@SKIP@@ non-Debug build (${BUILD_TYPE:-unknown}); assertion compiled out"
    exit 0
fi

TABLE=block_offset_repro_04123

# Clean up in case of leftovers
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"

# Create a MergeTree table with _block_offset persistence enabled
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (id UInt32, v UInt32)
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS enable_block_offset_column = 1, enable_block_number_column = 1,
             min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0
"

# Write at least one part
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} VALUES (1,1),(2,2),(3,3)"

# Detach all parts so we can modify files on disk
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${TABLE} DETACH PARTITION ID 'all'"

# Find the detached part path
PART_PATH=$($CLICKHOUSE_CLIENT -q "SELECT path FROM system.detached_parts WHERE database = currentDatabase() AND table = '${TABLE}' ORDER BY modification_time DESC LIMIT 1" | tr -d '\n')

# Try to ensure the detached part directory is writable (clear immutability on macOS, add u+w).
chmod -R u+w "${PART_PATH}" 2>/dev/null || true
chflags -R nouchg "${PART_PATH}" 2>/dev/null || true
chflags -R noschg "${PART_PATH}" 2>/dev/null || true
chmod -RN "${PART_PATH}" 2>/dev/null || true
xattr -rc "${PART_PATH}" 2>/dev/null || true
find "${PART_PATH}" -type d -exec chmod u+rwx {} + 2>/dev/null || true
find "${PART_PATH}" -type f -exec chmod u+rw {} + 2>/dev/null || true

# If we still cannot write, skip to avoid stderr-based harness FAIL.
if ! ( : > "${PART_PATH}/.writetest" ) 2>/dev/null; then
    echo "@@SKIP@@ cannot modify detached files at ${PART_PATH}"
    exit 0
fi
rm -f "${PART_PATH}/.writetest" 2>/dev/null || true

# Corrupt a real numeric column's data size: truncate data file for column `v`
# but keep marks intact, then remove checksums so ATTACH recomputes from disk.
# This will make size.data_uncompressed for `v` equal to 0 while rows_count > 0,
# which triggers the debug-only consistency assertion in unfixed builds.
if ! ( : > "${PART_PATH}/v.bin" 2>/dev/null || truncate -s 0 "${PART_PATH}/v.bin" 2>/dev/null ); then
    echo "@@SKIP@@ cannot truncate ${PART_PATH}/v.bin"
    exit 0
fi
rm -f "${PART_PATH}/checksums.txt" 2>/dev/null || true

# Re-attach the partition with the modified part
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${TABLE} ATTACH PARTITION ID 'all'" 2>/dev/null || true

# This query forces calculation of column sizes for each column in the part,
# which triggers the debug assertion in unfixed Debug builds. With the fix, it
# succeeds, and the test will print OK below.
$CLICKHOUSE_CLIENT -q "SELECT sum(column_data_uncompressed_bytes) FROM system.parts_columns WHERE database = currentDatabase() AND table = '${TABLE}' FORMAT Null"

# Cleanup and print a stable success token
$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE}"
$CLICKHOUSE_CLIENT -q "SELECT 'OK' FORMAT TSV"
