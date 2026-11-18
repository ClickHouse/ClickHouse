#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `URL` engine dispatches non-HTTP schemes to a delegate (File/object storage) wrapped in
# `StorageURLSchemeDispatch`. The wrapper must preserve the delegate's planner-visible capability
# contracts, otherwise `ENGINE = URL('file://...')` would accept optimizations the direct backend
# (and the plain `URL` engine) deliberately reject:
#   * subcolumn optimization (`supportsOptimizationToSubcolumns`) — `StorageFile` disables it;
#   * `PREWHERE` on default-expression columns (`supportedPrewhereColumns`) — `StorageFile` excludes them.

CSV="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
CSV_ABS="${USER_FILES_PATH}/${CSV}"
printf '"[1,2,3]"\n"[4,5]"\n' > "$CSV_ABS"

echo "--- control: a MergeTree table advertises the optimization, so length(arr) is rewritten to arr.size0 ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_mt"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_mt (arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "EXPLAIN QUERY TREE SELECT length(arr) FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_mt SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1" 2>&1 \
    | grep -qiE "arr\.size0" && echo "control-rewritten" || echo "control-NOT-rewritten"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_mt"

echo "--- ENGINE = URL('file://...') delegates to File, which disables the optimization: length(arr) is NOT rewritten ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_sub"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_sub (arr Array(UInt32)) ENGINE = URL('file://${CSV_ABS}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "EXPLAIN QUERY TREE SELECT length(arr) FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_sub SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1" 2>&1 \
    | grep -qiE "arr\.size0" && echo "subcolumn-rewritten (BUG)" || echo "subcolumn-not-rewritten"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_sub"

# `PREWHERE` is only supported by the Parquet format, so the rest of the test uses a Parquet file.
PARQ="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
PARQ_ABS="${USER_FILES_PATH}/${PARQ}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO TABLE FUNCTION file('${PARQ}', 'Parquet', 'a UInt32') VALUES (1), (2)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_pw"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_pw (a UInt32, b UInt32 DEFAULT a + 1) ENGINE = URL('file://${PARQ_ABS}', 'Parquet')"

echo "--- PREWHERE on a plain column is allowed (delegate's supportedPrewhereColumns contains it) ---"
${CLICKHOUSE_CLIENT} -q "SELECT a FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_pw PREWHERE a = 1 ORDER BY a"

echo "--- PREWHERE on a default-expression column is rejected (delegate's supportedPrewhereColumns excludes it) ---"
${CLICKHOUSE_CLIENT} -q "SELECT a FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_pw PREWHERE b = 2" 2>&1 \
    | grep -qiE "does not support column .* in PREWHERE|ILLEGAL_PREWHERE" && echo "prewhere-default-col-rejected" || echo "NOT REJECTED"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_pw"

rm -f "$CSV_ABS" "$PARQ_ABS"
