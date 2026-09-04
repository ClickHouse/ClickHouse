#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104891
# (dependent-sink path).
#
# Companion to `04230_iceberg_optimize_metadata_not_initialized_104711` (the
# parent regression test for ClickHouse#104711). When an INSERT lands on a
# non-datalake source table that has a `MATERIALIZED VIEW` whose target is a
# freshly attached data lake table with uninitialized `current_metadata`, the
# dependency builder hits `StorageObjectStorage::supportsParallelInsert` on the
# target before any call site refreshes the data lake metadata.
#
# Direct `INSERT INTO <datalake>` is gated by
# `InterpreterInsertQuery::execute` calling
# `table->updateExternalDynamicMetadataIfExists` on the root insert target
# (`src/Interpreters/InterpreterInsertQuery.cpp:1027`), so the direct shape
# initializes the metadata before `supportsParallelInsert` runs. The
# dependent-sink path is different: `InsertDependenciesBuilder` walks the
# dependency tree of the root table and queries
# `storage->supportsParallelInsert` on each non-view sink at
# `src/Interpreters/InsertDependenciesBuilder.cpp:768`, but
# `updateExternalDynamicMetadataIfExists` is only called once for the root
# table - any data lake target reached through a MV remains uninitialized.
#
# Without PR #104917's lazy-init guard in
# `StorageObjectStorage::supportsParallelInsert`, this raises
# `LOGICAL_ERROR: 'Metadata is not initialized'` from `assertInitialized`,
# fatal in debug / sanitizer builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/test_104891_mv_${CLICKHOUSE_DATABASE}"
trap "rm -rf '${BASE_DIR}' 2>/dev/null" EXIT
mkdir -p "${BASE_DIR}"

TARGET="datalake_target_${RANDOM}"
SOURCE="mv_source_${RANDOM}"
MV="mv_${RANDOM}"
TARGET_PATH="${BASE_DIR}/${TARGET}/"

# Step 1: create the data lake target. This writes the initial metadata file.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TARGET} (c0 Int32)
    ENGINE = IcebergLocal('${TARGET_PATH}')
"

# Step 2: corrupt every metadata file on disk. This is done directly rather than
# by provoking a failed write: writes to `IcebergLocal` go through a temporary
# file and `rename`, so a failed write leaves no partial file at the target path.
corrupted=0
for metadata_file in "${TARGET_PATH}"metadata/*.metadata.json; do
    [ -f "${metadata_file}" ] || continue
    echo 'not a json' > "${metadata_file}"
    corrupted=1
done
[ "${corrupted}" = 1 ] && echo "[mv_to_datalake] target metadata corrupted"

# Step 3: build the dependency chain - a plain `MergeTree` source plus a
# `MATERIALIZED VIEW` that routes rows to the corrupted data lake target.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${SOURCE} (c0 Int32) ENGINE = MergeTree ORDER BY tuple()
"
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${MV} TO ${TARGET} AS SELECT c0 FROM ${SOURCE}
"

# Step 4: DETACH+ATTACH the data lake target to clear the in-memory
# `current_metadata` cache. Silence the load-failure warning the client
# forwards to its stderr.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TARGET} SYNC"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal \
    --query "ATTACH TABLE ${TARGET}" 2>/dev/null

# Step 5: INSERT into the source. The dependency builder will reach
# `StorageObjectStorage::supportsParallelInsert` on the freshly attached
# target without any prior metadata refresh. The INSERT may legitimately
# fail with a user-facing exception when the metadata load reveals the
# corruption - we only assert that the server stays alive. Without the
# `supportsParallelInsert` lazy-init guard this raises
# `LOGICAL_ERROR: 'Metadata is not initialized'`, which `chassert` /
# `Logical error` paths turn into SIGABRT in debug and sanitizer builds.
INSERT_OUTPUT=$(${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 \
    --query "INSERT INTO ${SOURCE} VALUES (3)" 2>&1 || true)
if echo "${INSERT_OUTPUT}" | grep -F 'Logical error' > /dev/null; then
    echo "[mv_to_datalake] FAIL: client received Logical error"
else
    echo "[mv_to_datalake] client did not receive Logical error"
fi

# The decisive check - is the server still serving queries?
if [[ "$(${CLICKHOUSE_CLIENT} --connect_timeout=3 --query "SELECT 1" 2>/dev/null)" == "1" ]]; then
    echo "[mv_to_datalake] server alive after dependent-sink INSERT"
else
    echo "[mv_to_datalake] FAIL: server unreachable after dependent-sink INSERT"
fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${MV} SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${SOURCE} SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TARGET} SYNC"
