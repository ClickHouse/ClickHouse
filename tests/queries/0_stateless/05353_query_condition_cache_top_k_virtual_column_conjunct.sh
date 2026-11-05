#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# An `ORDER BY ... LIMIT` read of a `File` table must not reuse the query condition cache entries of a
# query that differs only in a conjunct on a Hive partition or virtual column. Such conjuncts are not
# pushed into the format, but they decide which rows make the TopK threshold.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05353"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/key=9/data.parquet', Parquet, 'v Int64') SELECT number FROM numbers(1000)
    SETTINGS output_format_parquet_row_group_size = 100"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/key=9/data.parquet"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05353 (key Int64, v Int64) ENGINE = File(Parquet, '${DATA_DIR}/key=9/data.parquet')"

SETTINGS="use_hive_partitioning = 1, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
    query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1"

for condition in "key = 1 OR v > 180" "key = 1 OR v > 450" "_file = 'x' OR v > 600" "key = 9" "key = 9 AND v > 700"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(v) FROM (SELECT v FROM t_05353 WHERE (${condition}) AND v < 100000 ORDER BY v LIMIT 3) SETTINGS ${SETTINGS}"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05353"
rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
