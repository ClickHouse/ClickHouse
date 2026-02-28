#!/usr/bin/env bash
# Regression test for debug assertion in calculateEachColumnSizes.
#
# The assertion fires when a numeric column is declared in columns.txt but has
# no data file on disk. This legitimately happens when concurrent ALTER
# ADD/MODIFY/DROP COLUMN and mutations produce a part with such a mismatch.
#
# This test deterministically creates the problematic state by:
# 1. Creating a wide part with data
# 2. Detaching it
# 3. Adding a numeric column (h UInt64) to columns.txt without creating its data
# 4. Re-attaching and querying system.parts_columns to trigger the assertion
#
# In Debug builds WITHOUT the fix, this throws LOGICAL_ERROR.
# With the fix (or in Release), it succeeds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error
. "$CURDIR"/../shell_config.sh

set -euo pipefail

TABLE="column_sizes_assert_03815"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"

# Create a table that produces wide parts
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (a UInt32, b UInt64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0
"

# Insert data to create a part with some rows
$CLICKHOUSE_CLIENT -q "INSERT INTO ${TABLE} SELECT number, number * 10 FROM numbers(100)"

# Detach the part so we can modify files on disk
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${TABLE} DETACH PARTITION ID 'all'"

# Find the detached part path
PART_PATH=$($CLICKHOUSE_CLIENT -q "
    SELECT path
    FROM system.detached_parts
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY name
    LIMIT 1
" | tr -d '\n')

if [[ -z "${PART_PATH}" ]]; then
    echo "FAIL: no detached part found"
    exit 1
fi

COLUMNS_FILE="${PART_PATH}/columns.txt"
if [[ ! -f "${COLUMNS_FILE}" ]]; then
    echo "FAIL: columns.txt not found at ${COLUMNS_FILE}"
    exit 1
fi

# Add column h UInt64 to columns.txt without creating h.bin data file.
# The format of columns.txt is:
#   columns format version: 1
#   N columns:
#   `col1` Type1
#   `col2` Type2
#   ...
# We increment N and append `h` UInt64.
ORIG_COUNT=$(sed -n '2s/ columns://p' "${COLUMNS_FILE}" | tr -d '[:space:]')
NEW_COUNT=$((ORIG_COUNT + 1))
sed -i "2s/${ORIG_COUNT} columns:/${NEW_COUNT} columns:/" "${COLUMNS_FILE}"
echo '`h` UInt64' >> "${COLUMNS_FILE}"

# Checksums.txt is left as-is. It won't have an entry for h.bin (since we
# never created the file), but columns.txt now lists h. This mismatch is
# exactly what happens in the race condition scenario.

# Re-attach the part. The server reads columns.txt (with h) and checksums.txt
# (without h.bin) and loads the part successfully.
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${TABLE} ATTACH PARTITION ID 'all'" 2>/dev/null || true

# This query forces per-column size calculation which triggers the debug assertion.
# In Debug builds without the fix: LOGICAL_ERROR exception
# (Column h has rows count 0 ... but data part has 100 rows).
# With the fix: succeeds (zero-size columns are skipped in the assertion).
$CLICKHOUSE_CLIENT -q "
    SELECT sum(data_uncompressed_bytes)
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = '${TABLE}'
    FORMAT Null
"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE}"
echo "OK"
