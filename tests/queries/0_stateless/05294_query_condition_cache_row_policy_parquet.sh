#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet.
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment.

# A row policy is applied inside the Parquet reader, and `getMatchedBuckets` records a row group
# with `rows_pass == 0` as unmatched. The cache key must include that policy. Otherwise a later
# read with a different policy reuses the `WHERE`-only verdict and skips real rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_hide="u_hide_${CLICKHOUSE_DATABASE}"
user_show="u_show_${CLICKHOUSE_DATABASE}"
DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05294_query_condition_cache_row_policy.parquet"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user_hide}, ${user_show}"
mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP TABLE IF EXISTS t_qcc_row_policy_parquet;
CREATE TABLE t_qcc_row_policy_parquet (k UInt64, x UInt8)
ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05294_query_condition_cache_row_policy.parquet')
SETTINGS output_format_parquet_row_group_size = 10000;

INSERT INTO t_qcc_row_policy_parquet
SELECT number, if(number < 10000, 0, 1) FROM numbers(20000);

CREATE USER ${user_hide} NOT IDENTIFIED;
CREATE USER ${user_show} NOT IDENTIFIED;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet TO ${user_hide}, ${user_show};

CREATE ROW POLICY rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet FOR SELECT USING x = 0 TO ${user_hide};
CREATE ROW POLICY rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet FOR SELECT USING 1 TO ${user_show};
"

# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

echo "row_groups:"
${CLICKHOUSE_CLIENT} --query "
SELECT num_row_groups FROM file('${CLICKHOUSE_DATABASE}/05294_query_condition_cache_row_policy.parquet', ParquetMetadata)
"

COMMON="use_query_condition_cache = 1, optimize_count_from_files = 0, use_cache_for_count_from_files = 0, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1, max_threads = 1, enable_analyzer = 1"

echo "hide user (x = 0), expect 10000:"
${CLICKHOUSE_CLIENT} --user "${user_hide}" --query "
SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet WHERE k >= 0
SETTINGS ${COMMON}
"

echo "show user (always-true policy), same WHERE, expect 20000:"
${CLICKHOUSE_CLIENT} --user "${user_show}" --query "
SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet WHERE k >= 0
SETTINGS ${COMMON}
"

qid="${CLICKHOUSE_TEST_UNIQUE_NAME:-05294}_hide_again"
echo "hide user again, expect 10000:"
out=$(${CLICKHOUSE_CLIENT} --user "${user_hide}" --query_id="$qid" --print-profile-events -q "
SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet WHERE k >= 0
SETTINGS ${COMMON}
" 2>&1)
printf '%s\n' "$out" | awk '/^[0-9]+$/ { print; exit }'
hits=$(printf '%s\n' "$out" | awk '/QueryConditionCacheHits:/ { print $(NF-1); exit }')
echo "repeated hide query was a cache hit (expect 1):"
if [ "${hits:-0}" -gt 0 ]; then
    echo 1
else
    echo 0
fi

${CLICKHOUSE_CLIENT} --multiquery --query "
DROP ROW POLICY IF EXISTS rp_hide_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet;
DROP ROW POLICY IF EXISTS rp_show_${CLICKHOUSE_DATABASE} ON ${CLICKHOUSE_DATABASE}.t_qcc_row_policy_parquet;
DROP TABLE IF EXISTS t_qcc_row_policy_parquet;
DROP USER IF EXISTS ${user_hide}, ${user_show};
"
