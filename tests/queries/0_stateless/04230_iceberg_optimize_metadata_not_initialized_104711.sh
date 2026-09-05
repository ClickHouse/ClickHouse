#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104711:
# `OPTIMIZE TABLE` on a freshly attached `IcebergLocal` table whose metadata
# could not be loaded used to raise `Logical error: 'Metadata is not initialized'`
# from `DataLakeConfiguration::optimize`. After the fix, it raises a regular
# user-facing exception describing the underlying load failure.
#
# We reproduce the "could not load metadata" state by:
#   1. creating an Iceberg table,
#   2. overwriting its metadata file on disk with a non-JSON payload,
#   3. `DETACH ... SYNC` + `ATTACH` (equivalent to a server restart from the
#      perspective of `DataLakeConfiguration::current_metadata`),
#   4. then issuing `OPTIMIZE TABLE` / `ALTER TABLE` / `SELECT` which previously
#      crashed.
#
# The metadata is corrupted directly rather than by provoking a failed write:
# writes to `IcebergLocal` go through a temporary file and `rename`, so a failed
# write leaves no partial file at the target path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

# Step 1: create the table. This writes the initial metadata file.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

# Step 2: corrupt every metadata file on disk.
corrupted=0
for metadata_file in "${TABLE_PATH}"metadata/*.metadata.json; do
    [ -f "${metadata_file}" ] || continue
    echo 'not a json' > "${metadata_file}"
    corrupted=1
done
[ "${corrupted}" = 1 ] && echo "metadata corrupted"

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
