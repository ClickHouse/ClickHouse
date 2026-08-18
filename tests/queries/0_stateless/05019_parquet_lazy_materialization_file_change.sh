#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05019_lazy_mat_file_change_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${LOCAL_DIR}/data.parquet', Parquet)
    SELECT number AS k, concat('old_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION file('${LOCAL_DIR}/replacement.parquet', Parquet)
    SELECT number AS k, concat('new_', toString(number)) AS s
    FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

# `sleepEachRow` keeps the main pass open while a replacement is atomically installed. The
# lazy pass must reject the replacement instead of combining columns from the two generations.
"${LOCAL[@]}" \
    --enable_analyzer=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_file=1 \
    --query "SELECT s FROM file('${LOCAL_DIR}/data.parquet', Parquet) PREWHERE sleepEachRow(0.01) = 0 ORDER BY k LIMIT 1" \
    > "${LOCAL_DIR}/query.out" 2>&1 &
query_pid=$!

sleep 1
mv "${LOCAL_DIR}/replacement.parquet" "${LOCAL_DIR}/data.parquet"
wait "${query_pid}" || true

if grep -q 'FILE_CHANGED_DURING_READ' "${LOCAL_DIR}/query.out"; then
    echo 'FILE_CHANGED_DURING_READ'
else
    cat "${LOCAL_DIR}/query.out" >&2
    exit 1
fi
