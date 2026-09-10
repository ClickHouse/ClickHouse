#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-parallel: this test uses the global `lance_metadata_iterate_pause` failpoint.

set -e -o pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SOURCE_DIR="${CUR_DIR}/data_lance"
FIXTURE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/data_lance"
TARGET_DIR="${FIXTURE_DIR}/recreated.lance"
TABLE_NAME="lance_snapshot_identity_${CLICKHOUSE_TEST_UNIQUE_NAME//[^a-zA-Z0-9_]/_}"
ERROR_FILE="${CLICKHOUSE_TMP}/lance_snapshot_identity_${CLICKHOUSE_TEST_UNIQUE_NAME}.stderr"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
mkdir -p "${FIXTURE_DIR}"
cp -R "${SOURCE_DIR}/basic.lance" "${TARGET_DIR}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT lance_metadata_iterate_pause" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_NAME}" >/dev/null 2>&1 || true
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
    rm -f "${ERROR_FILE}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE_NAME}
    ENGINE = LanceLocal('${TARGET_DIR}')"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT lance_metadata_iterate_pause"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE_NAME} FORMAT Null" 2>"${ERROR_FILE}" &
QUERY_PID=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT lance_metadata_iterate_pause PAUSE"

rm -rf "${TARGET_DIR}"
cp -R "${SOURCE_DIR}/pushdown.lance" "${TARGET_DIR}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT lance_metadata_iterate_pause"

if wait "${QUERY_PID}"; then
    echo "FAIL: recreated dataset was read through an old snapshot"
elif grep -q "snapshot identity mismatch" "${ERROR_FILE}"; then
    echo "OK"
else
    echo "FAIL: query failed without a snapshot identity mismatch"
    cat "${ERROR_FILE}"
fi
