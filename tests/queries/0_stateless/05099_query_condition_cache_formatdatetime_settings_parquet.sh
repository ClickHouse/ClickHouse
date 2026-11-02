#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: needs Parquet
# Tag no-parallel: asserts `QueryConditionCacheHits` on the instance-wide query condition cache,
# which a parallel sibling test can wipe at any moment (see 04498_query_condition_cache_local_files.sh).

# The `formatdatetime_*` settings change how `formatDateTime` evaluates without leaving a trace in the
# condition's `ActionsDAG`. The `File` / object storage readers build their query condition cache key in
# `FormatFilterInfo`, separately from `MergeTree`, so they need their own regression: two queries over a
# Parquet file that differ only in `formatdatetime_f_prints_single_zero` must not share the first query's
# "no row groups match" verdict.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05099_query_condition_cache_formatdatetime.parquet"

# Several row groups, so there is something to record: a row group that matches is never written to the cache.
# The reader only reports which row groups matched for a condition it evaluates itself, i.e. a PREWHERE one, and
# the optimizer only moves the condition there when the query reads more columns than the condition uses.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_qcc_formatdatetime_parquet"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_qcc_formatdatetime_parquet (k UInt64, d DateTime)
    ENGINE = File(Parquet, '${CLICKHOUSE_DATABASE}/05099_query_condition_cache_formatdatetime.parquet')
    SETTINGS output_format_parquet_row_group_size = 100000
"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO t_qcc_formatdatetime_parquet
    SELECT number, toDateTime('2024-05-05 10:00:00') + number % 86400 FROM numbers(1000000)
"

# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "$DATA_FILE"

# The reader only evaluates the condition itself, and therefore only records a verdict, when the optimizer
# moves it to PREWHERE. The stateless tests randomize the settings that control that, so pin them; the query
# condition cache also needs the analyzer.
COMMON_SETTINGS="enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

qid_default="${CLICKHOUSE_TEST_UNIQUE_NAME}_default"
qid_default_again="${CLICKHOUSE_TEST_UNIQUE_NAME}_default_again"

# `formatDateTime(d, '%f')` renders '000000' by default and '0' with the setting enabled, so the condition
# matches no row under the first value and every row under the second one.
echo "no row matches by default (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_default" --query "
    SELECT sum(k) FROM t_qcc_formatdatetime_parquet WHERE formatDateTime(d, '%f') = '0'
    SETTINGS ${COMMON_SETTINGS}, use_query_condition_cache = 1, formatdatetime_f_prints_single_zero = 0
"
echo "every row matches with the setting flipped, the verdict above must not be reused (expect 499999500000):"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(k) FROM t_qcc_formatdatetime_parquet WHERE formatDateTime(d, '%f') = '0'
    SETTINGS ${COMMON_SETTINGS}, use_query_condition_cache = 1, formatdatetime_f_prints_single_zero = 1
"
echo "the same without the cache (expect 499999500000):"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(k) FROM t_qcc_formatdatetime_parquet WHERE formatDateTime(d, '%f') = '0'
    SETTINGS ${COMMON_SETTINGS}, use_query_condition_cache = 0, formatdatetime_f_prints_single_zero = 1
"

# The cache was really engaged: the first query stored its verdict, and a repeat with the same settings reads it.
echo "repeated default query (expect 0):"
${CLICKHOUSE_CLIENT} --query_id="$qid_default_again" --query "
    SELECT sum(k) FROM t_qcc_formatdatetime_parquet WHERE formatDateTime(d, '%f') = '0'
    SETTINGS ${COMMON_SETTINGS}, use_query_condition_cache = 1, formatdatetime_f_prints_single_zero = 0
"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "first default query was a cache miss (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheMisses'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_default' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"
echo "repeated default query was a cache hit (expect 1):"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE query_id = '$qid_default_again' AND current_database = currentDatabase() AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_qcc_formatdatetime_parquet"
rm -f "$DATA_FILE"
