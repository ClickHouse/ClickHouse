#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization for `ORDER BY ... LIMIT n` queries over Iceberg tables with Parquet
# data files. The row identity used by the optimization is the physical row number within a
# data file, so it must stay correct in the presence of position deletes.
# Every query is run with the optimization enabled and disabled; the results must match.

TABLE="t_${CLICKHOUSE_DATABASE}_lazy_mat"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (k UInt64, f UInt64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# Two data files.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --output_format_parquet_row_group_size=500 --query "
    INSERT INTO ${TABLE}
    SELECT number AS k, number % 7 AS f, concat('val_', toString(number)) AS s FROM numbers(5000)
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --output_format_parquet_row_group_size=500 --query "
    INSERT INTO ${TABLE}
    SELECT number AS k, number % 7 AS f, concat('val_', toString(number)) AS s FROM numbers(5000, 5000)
"

# Position delete files: remove every hundredth row.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    ALTER TABLE ${TABLE} DELETE WHERE k % 100 = 0
"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer
# (see `QueryPlanOptimizationSettings`), and some CI configurations run with the old analyzer.
run_with_both_settings()
{
    local query=$1
    for enabled in 1 0; do
        ${CLICKHOUSE_CLIENT} \
            --enable_analyzer=1 \
            --query_plan_optimize_lazy_materialization=1 \
            --query_plan_max_limit_for_lazy_materialization=0 \
            --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" \
            --query "$query"
    done
}

echo '-- simple ORDER BY ... LIMIT (the first rows are deleted)'
run_with_both_settings "SELECT k, s FROM ${TABLE} ORDER BY k LIMIT 3"

echo '-- ORDER BY ... DESC LIMIT across data files'
run_with_both_settings "SELECT k, s FROM ${TABLE} ORDER BY k DESC LIMIT 3"

echo '-- with a filter'
run_with_both_settings "SELECT k, s FROM ${TABLE} WHERE f = 3 ORDER BY k LIMIT 3"

echo '-- rows adjacent to deleted rows keep their values'
run_with_both_settings "SELECT k, s FROM ${TABLE} WHERE k BETWEEN 4998 AND 5002 ORDER BY k LIMIT 5"

echo '-- the lazy read step is in the plan only when the optimization is enabled'
${CLICKHOUSE_CLIENT} \
    --enable_analyzer=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN SELECT s FROM ${TABLE} ORDER BY k LIMIT 3" | grep -c 'LazilyReadFromObjectStorage'
${CLICKHOUSE_CLIENT} \
    --enable_analyzer=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=0 \
    --query "EXPLAIN SELECT s FROM ${TABLE} ORDER BY k LIMIT 3" | grep -c 'LazilyReadFromObjectStorage' || true

# Schema evolution forces reading all physical columns of the older files, so such
# snapshots must not take the lazy path even when the optimization is enabled.
echo '-- schema evolution keeps the lazy path off'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} ADD COLUMN extra Nullable(UInt64)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE}
    SELECT number AS k, number % 7 AS f, concat('val_', toString(number)) AS s, number * 2 AS extra FROM numbers(10000, 100)
"
${CLICKHOUSE_CLIENT} \
    --enable_analyzer=1 \
    --query_plan_optimize_lazy_materialization=1 \
    --query_plan_max_limit_for_lazy_materialization=0 \
    --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN SELECT s FROM ${TABLE} ORDER BY k LIMIT 3" | grep -c 'LazilyReadFromObjectStorage' || true
run_with_both_settings "SELECT k, s, extra FROM ${TABLE} ORDER BY k DESC LIMIT 3"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
