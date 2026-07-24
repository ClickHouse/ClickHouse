#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104711:
# `OPTIMIZE TABLE` on a freshly attached `IcebergLocal` table whose metadata
# could not be loaded used to raise `Logical error: 'Metadata is not initialized'`
# from `DataLakeConfiguration::optimize`. After the fix, it raises a regular
# user-facing exception describing the underlying load failure.
#
# We reproduce the "could not load metadata" state by:
#   1. creating an Iceberg table with `iceberg_metadata_compression_method='deflate'`,
#   2. issuing an `INSERT` with an invalid `output_format_compression_level=11`
#      so the second metadata file lands on disk corrupted,
#   3. `DETACH ... SYNC` + `ATTACH` (equivalent to a server restart from the
#      perspective of `DataLakeConfiguration::current_metadata`),
#   4. then issuing `OPTIMIZE TABLE` / `ALTER TABLE` / `SELECT` which previously
#      crashed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# Step 1: create the table with the deflate metadata format.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    SETTINGS iceberg_metadata_compression_method = 'deflate'
"

# Step 2: provoke the corrupt-metadata-on-disk state via an INSERT with an
# unsupported compression level. The INSERT itself must fail.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --output_format_compression_level=11 \
    --query "INSERT INTO ${TABLE} VALUES (2)" 2>&1 \
    | grep -F 'INCORRECT_DATA' > /dev/null && echo "INSERT failed as expected"

# Step 3: detach + attach to clear the in-memory `current_metadata` cache,
# the way a server restart would. The corrupted metadata produces a server-side
# `<Warning>` log during ATTACH (swallowed by the lazy-init catch in
# `StorageObjectStorage`'s constructor), so silence the log channel that the
# client forwards to its stderr — otherwise the test fails on "having stderror".
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} SYNC"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}" 2>/dev/null

# Step 4: the previously-crashing operations must now raise a regular
# exception instead of `LOGICAL_ERROR`. We do not care which specific
# error code is reported (it depends on what `update` happens to fail
# with for the corrupted metadata) — only that it is NOT a logical error
# and that the server keeps running.
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 \
    --query "OPTIMIZE TABLE ${TABLE}" 2>&1 \
    | grep -F 'Logical error' > /dev/null && echo "FAIL: OPTIMIZE crashed with Logical error" \
    || echo "OPTIMIZE did not crash with Logical error"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "ALTER TABLE ${TABLE} DELETE WHERE c0 = 0" 2>&1 \
    | grep -F 'Logical error' > /dev/null && echo "FAIL: ALTER crashed with Logical error" \
    || echo "ALTER did not crash with Logical error"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ADD COLUMN c1 Int32" 2>&1 \
    | grep -F 'Logical error' > /dev/null && echo "FAIL: ALTER ADD COLUMN crashed with Logical error" \
    || echo "ALTER ADD COLUMN did not crash with Logical error"

# The server is still alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
