#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104891.
#
# `InterpreterDeleteQuery::execute` calls `table->supportsDelete()` BEFORE
# `table->checkMutationIsPossible()` for `DELETE FROM ... WHERE ...` queries.
# For a freshly attached data lake table (Iceberg / DeltaLake / Hudi) whose
# `current_metadata` has not been loaded yet (e.g. because the previous load
# failed and was swallowed by the lazy-init catch in `StorageObjectStorage`'s
# constructor), `DataLakeConfiguration::supportsDelete()` used to throw
# `LOGICAL_ERROR: 'Metadata is not initialized'` from `assertInitialized()`,
# which is fatal in debug / sanitizer builds.
#
# This is the sibling case of the issue fixed for OPTIMIZE / ALTER by #104738
# (see test 04230_iceberg_optimize_metadata_not_initialized_104711.sh).
# The `supportsDelete()` call site was not covered by that fix because it is
# reached before any of the four methods updated there. The current test
# isolates it by starting with `DELETE FROM` immediately after `ATTACH`,
# without any preceding `OPTIMIZE` / `ALTER` that would lazy-initialize the
# metadata first.
#
# Reproduction setup (mirrors 04230):
#   1. Create an Iceberg table with `iceberg_metadata_compression_method='deflate'`.
#   2. Issue an `INSERT` with an invalid `output_format_compression_level=11`
#      so the second metadata file lands on disk corrupted.
#   3. `DETACH ... SYNC` + `ATTACH` (clears `current_metadata`, equivalent to
#      a server restart from the table engine's perspective).
#   4. Issue `DELETE FROM table WHERE ...` which previously raised
#      `Logical error: 'Metadata is not initialized'`.
#
# After the fix, step 4 raises a regular user-facing exception describing
# the underlying load failure (`INCORRECT_DATA` for the corrupted metadata
# file) and the server keeps running.

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

# Step 3: detach + attach to clear the in-memory `current_metadata` cache.
# The corrupted metadata produces a server-side `<Warning>` log during ATTACH
# (swallowed by the lazy-init catch in `StorageObjectStorage`'s constructor),
# so silence the log channel that the client forwards to its stderr.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} SYNC"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}" 2>/dev/null

# Step 4: the previously-crashing operation must now raise a regular exception
# instead of `LOGICAL_ERROR`. We do not care which specific error code is
# reported (it depends on what `update` happens to fail with for the corrupted
# metadata) — only that it is NOT a logical error and that the server keeps
# running.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "DELETE FROM ${TABLE} WHERE c0 = 0" 2>&1 \
    | grep -F 'Logical error' > /dev/null && echo "FAIL: DELETE FROM crashed with Logical error" \
    || echo "DELETE FROM did not crash with Logical error"

# The server is still alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
