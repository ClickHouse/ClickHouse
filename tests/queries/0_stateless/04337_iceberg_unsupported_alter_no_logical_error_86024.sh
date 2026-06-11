#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: uses DETACH/ATTACH which serializes per database

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/86024:
# unsupported ALTER / mutation / OPTIMIZE / MODIFY SETTING statements on an
# Iceberg table used to abort with `Code: 49 ... Metadata is not initialized.
# (LOGICAL_ERROR)` (or a <Fatal> SIGABRT) instead of a regular user-facing
# error. The crash fired from `DataLakeConfiguration::{checkAlterIsPossible,
# checkMutationIsPossible,alter,optimize}` whenever `current_metadata` had not
# been lazily loaded yet, because those paths called `assertInitialized()`
# before the intended "not supported by Iceberg storage" rejection.
#
# A freshly DETACH+ATTACH'ed table reproduces the `current_metadata == nullptr`
# state (the same as a server restart), so running each statement as the FIRST
# operation after ATTACH exercises the original crash path. We only assert that
# none of them raise a logical error and that the server stays alive; the exact
# error code is irrelevant and may change as Iceberg gains support for these
# statements.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"
# Produce at least one real snapshot so the statements are not no-ops.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"

# Run a single statement as the FIRST operation after ATTACH, i.e. with
# `current_metadata == nullptr`. Assert it does not crash with a logical error
# and that the server is still responsive afterwards.
check() {
    local label="$1"
    local query="$2"
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} SYNC"
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}" 2>/dev/null
    if ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "${query}" 2>&1 | grep -qF 'Logical error'; then
        echo "FAIL: ${label} raised a logical error"
    else
        echo "OK: ${label}"
    fi
}

check "ADD COLUMN"               "ALTER TABLE ${TABLE} ADD COLUMN c1 Int32"
check "DROP COLUMN"              "ALTER TABLE ${TABLE} DROP COLUMN c0"
check "RENAME COLUMN"            "ALTER TABLE ${TABLE} RENAME COLUMN c0 TO c0x"
check "CLEAR COLUMN"             "ALTER TABLE ${TABLE} CLEAR COLUMN c0"
check "MODIFY COLUMN type"       "ALTER TABLE ${TABLE} MODIFY COLUMN c0 Int64"
check "MODIFY COLUMN comment"    "ALTER TABLE ${TABLE} MODIFY COLUMN c0 COMMENT 'x'"
check "UPDATE mutation"          "ALTER TABLE ${TABLE} UPDATE c0 = c0 + 1 WHERE 1"
check "DELETE mutation"          "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1"
check "lightweight DELETE"       "DELETE FROM ${TABLE} WHERE c0 = 1"
check "OPTIMIZE"                 "OPTIMIZE TABLE ${TABLE}"
check "MODIFY SETTING"           "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_metadata_file_path = 'x'"

# The server must still be alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
