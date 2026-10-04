#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads a Parquet file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05055_lazy_matcher_default_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")
DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "${DATA_DIR}"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/defaults.parquet', Parquet)
    SELECT number AS k, 100 - number AS a, concat('value_', toString(number)) AS s
    FROM numbers(10)
    SETTINGS engine_file_truncate_on_insert = 1;
"

QUERIES="
CREATE TABLE t_lazy_matcher_default
(
    k UInt64,
    a UInt64,
    s String,
    b UInt64 DEFAULT plus(COLUMNS('^a$'), 1)
)
ENGINE = File(Parquet, '${DATA_DIR}/defaults.parquet');
SELECT b, s FROM t_lazy_matcher_default ORDER BY b LIMIT 3;
SELECT countIf(explain LIKE '%LazilyReadFromFile%')
FROM (EXPLAIN SELECT b, s FROM t_lazy_matcher_default ORDER BY b LIMIT 3);
DROP TABLE t_lazy_matcher_default;
"

for enabled in 1 0; do
    echo "-- query_plan_optimize_lazy_materialization_for_file = ${enabled}"
    "${LOCAL[@]}" \
        --enable_analyzer=1 \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_file="${enabled}" \
        --query "${QUERIES}"
done
