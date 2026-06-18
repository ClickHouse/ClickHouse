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
# operation after ATTACH exercises the original crash path.
#
# We do NOT enable `allow_insert_into_iceberg` for the statement under test:
# that setting is the experimental gate, so without it every statement below is
# unsupported and must be REJECTED with a regular exception. Each statement must
# therefore (1) fail with a non-zero status (a status-0 success would mean the
# table was silently mutated instead of rejected) and (2) not raise a logical
# error. The exact error code is irrelevant and may change as Iceberg gains
# support for these statements, so it is not asserted.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
# Setup must fail the test loudly: a broken setup leaves the statement under test
# failing with an unrelated error that would be mistaken for a clean rejection.
# Two columns so DROP/CLEAR COLUMN reach the Iceberg storage path instead of the
# generic "Cannot DROP or CLEAR all columns" guard.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int32, c1 Int32)
    ENGINE = IcebergLocal('${TABLE_PATH}')
" || { echo "FAIL: CREATE TABLE failed"; exit 1; }
# Produce at least one real snapshot so the statements are not no-ops.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1, 2)" \
    || { echo "FAIL: INSERT failed"; exit 1; }

# Run a single statement as the FIRST operation after ATTACH, i.e. with
# `current_metadata == nullptr`. Assert the statement is rejected (non-zero
# status), does not crash with a logical error, and that the server is still
# responsive afterwards.
check() {
    local label="$1"
    local query="$2"
    local detach_out attach_out output status
    # DETACH/ATTACH reset current_metadata to nullptr; a failed setup step here
    # must fail the test, not leave the statement under test on a missing table.
    detach_out=$(${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE} SYNC" 2>&1) \
        || { echo "FAIL: ${label} setup DETACH failed: ${detach_out}"; exit 1; }
    attach_out=$(${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "ATTACH TABLE ${TABLE}" 2>&1) \
        || { echo "FAIL: ${label} setup ATTACH failed: ${attach_out}"; exit 1; }
    # No --allow_insert_into_iceberg: the statement must be rejected, not applied.
    output=$(${CLICKHOUSE_CLIENT} --query "${query}" 2>&1)
    status=$?
    if [ "${status}" -eq 0 ]; then
        # Status 0 means the statement was applied (e.g. the column was actually
        # added/renamed) instead of being rejected as unsupported.
        echo "FAIL: ${label} unexpectedly succeeded (status 0)"
    elif echo "${output}" | grep -qE 'Logical error|LOGICAL_ERROR'; then
        # Reject both forms: the debug fatal-log wording "Logical error" and the
        # release-build client exception code name "LOGICAL_ERROR" (Code: 49).
        echo "FAIL: ${label} raised a logical error"
    elif echo "${output}" | grep -qF 'UNKNOWN_TABLE'; then
        # The table must be attached when the statement runs, otherwise the
        # first-operation-after-ATTACH path was never exercised.
        echo "FAIL: ${label} hit UNKNOWN_TABLE (table not attached)"
    else
        echo "OK: ${label}"
    fi
}

check "ADD COLUMN"               "ALTER TABLE ${TABLE} ADD COLUMN c2 Int32"
check "DROP COLUMN"              "ALTER TABLE ${TABLE} DROP COLUMN c1"
check "RENAME COLUMN"            "ALTER TABLE ${TABLE} RENAME COLUMN c0 TO c0x"
check "CLEAR COLUMN"             "ALTER TABLE ${TABLE} CLEAR COLUMN c1"
check "MODIFY COLUMN type"       "ALTER TABLE ${TABLE} MODIFY COLUMN c0 Int64"
check "MODIFY COLUMN comment"    "ALTER TABLE ${TABLE} MODIFY COLUMN c0 COMMENT 'x'"
check "MODIFY ORDER BY"          "ALTER TABLE ${TABLE} MODIFY ORDER BY c0"
check "UPDATE mutation"          "ALTER TABLE ${TABLE} UPDATE c0 = c0 + 1 WHERE 1"
check "DELETE mutation"          "ALTER TABLE ${TABLE} DELETE WHERE c0 = 1"
check "lightweight DELETE"       "DELETE FROM ${TABLE} WHERE c0 = 1"
check "OPTIMIZE"                 "OPTIMIZE TABLE ${TABLE}"
check "MODIFY SETTING"           "ALTER TABLE ${TABLE} MODIFY SETTING iceberg_metadata_file_path = 'x'"

# The server must still be alive.
${CLICKHOUSE_CLIENT} --query "SELECT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC"
