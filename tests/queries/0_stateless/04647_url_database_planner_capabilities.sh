#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory and on the Parquet format.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a `URL` database is a `StorageProxy` around the storage created by the `url` table
# function. `StorageProxy` forwards only a part of the planner-visible capability contracts, so the
# proxy must forward the rest explicitly (the `URL` engine wrapper, `StorageURLSchemeDispatch`,
# carries the same overrides — see `04402_url_engine_dispatch_planner_capabilities` and
# `04403_url_engine_dispatch_trivial_count`). Otherwise a table of a `URL` database would behave
# differently from the same data read directly through `file`.

DB="db_${CLICKHOUSE_TEST_UNIQUE_NAME}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB} ENGINE = URL('file://')"

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
ARR="${CLICKHOUSE_USER_FILES_UNIQUE}/arr.csv"
printf '"[1,2,3]"\n"[4,5]"\n' > "${ARR}"

echo '--- supportsOptimizationToSubcolumns is forwarded: the File delegate disables it, so length() is not rewritten'
${CLICKHOUSE_CLIENT} -q "EXPLAIN QUERY TREE SELECT length(c1) FROM ${DB}.\`${ARR}\` SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1" 2>&1 \
    | grep -qiE "c1\.size0" && echo "subcolumn-rewritten (BUG)" || echo "subcolumn-not-rewritten"

# `PREWHERE` is only supported by the Parquet format, so the rest of the test uses Parquet files.
HIVE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/key=42"
mkdir -p "${HIVE_DIR}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO TABLE FUNCTION file('${HIVE_DIR}/hive.parquet', 'Parquet', 'a UInt32') VALUES (1), (2)"

echo '--- PREWHERE on a plain column is allowed (the delegate supportedPrewhereColumns contains it)'
${CLICKHOUSE_CLIENT} -q "SELECT a FROM ${DB}.\`${HIVE_DIR}/hive.parquet\` PREWHERE a = 1 SETTINGS use_hive_partitioning = 1"

echo '--- PREWHERE on a hive partition column is rejected (the delegate supportedPrewhereColumns excludes it)'
${CLICKHOUSE_CLIENT} -q "SELECT a FROM ${DB}.\`${HIVE_DIR}/hive.parquet\` PREWHERE key = 42 SETTINGS use_hive_partitioning = 1" 2>&1 \
    | grep -qiE "does not support column .* in PREWHERE|ILLEGAL_PREWHERE" && echo "prewhere-hive-col-rejected" || echo "NOT REJECTED"

# `supportsTrivialCountOptimization` gates `SelectQueryInfo::optimize_trivial_count`, which the
# delegate uses to take the row count from the Parquet metadata instead of reading the data. The
# row count is the same either way, so the observable difference is the amount of data read.
COUNT_FILE="${CLICKHOUSE_USER_FILES_UNIQUE}/count.parquet"
${CLICKHOUSE_CLIENT} -q "INSERT INTO TABLE FUNCTION file('${COUNT_FILE}', 'Parquet', 'a UInt32') SELECT number FROM numbers(1000)"

echo '--- supportsTrivialCountOptimization is forwarded: count() takes the count from the metadata'
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_on" -q \
    "SELECT count() FROM ${DB}.\`${COUNT_FILE}\` SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1"
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_off" -q \
    "SELECT count() FROM ${DB}.\`${COUNT_FILE}\` SETTINGS optimize_trivial_count_query = 0, optimize_count_from_files = 0"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
echo '--- the optimized count reads much less data than the full read (documents the mechanism)'
${CLICKHOUSE_CLIENT} -q "
SELECT countIf(query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_on' AND read_bytes < 100)
     + countIf(query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_off' AND read_bytes > 1000) = 2
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query_id IN ('${CLICKHOUSE_TEST_UNIQUE_NAME}_on', '${CLICKHOUSE_TEST_UNIQUE_NAME}_off')
  AND type = 'QueryFinish' AND event_date >= yesterday()"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
