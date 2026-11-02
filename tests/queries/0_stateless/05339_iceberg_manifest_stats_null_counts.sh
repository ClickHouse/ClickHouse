#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: a data file whose manifest entry has neither the NULL count nor the bounds of a column adds nothing to the
# statistics of that column, as a MergeTree part without the statistic, so its rows do not dilute the NULL fraction.
# Fixture written by Spark 3.5.5 / Iceberg 1.10: 2 data files of 1000 rows, `x` NULL in every other row, the second file
# inserted after `ALTER TABLE ... SET TBLPROPERTIES ('write.metadata.metrics.column.x' = 'none')`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The trace line is printed by join reordering, which the randomized settings can turn off.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --use_hash_table_stats_for_join_reordering=0 --enable_join_runtime_filters=0 --enable_parallel_replicas=0
    --use_iceberg_manifest_statistics=1 --use_statistics=1"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/null_counts_db/metrics_none_table" "${LAKE}/metrics_none_table"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE metrics_none ENGINE = IcebergLocal('${LAKE}/metrics_none_table');
    CREATE TABLE dim10 (k Int32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim10 SELECT number FROM numbers(10);"

echo '--- fixture: rows and the NULL count of x (field id 2) per data file'
${CLICKHOUSE_CLIENT} --query "
    SELECT record_count, mapContains(null_value_counts, 2), null_value_counts[2] FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'metrics_none' ORDER BY mapContains(null_value_counts, 2) DESC"
echo "NULL fraction: $(${CLICKHOUSE_CLIENT} --query "SELECT countIf(x IS NULL) / count() FROM metrics_none")"

echo '--- the Estimated statistics trace line of x'
echo 'Expect null 0.5: the fraction is over the first file (500 of 1000); counting the second as non-NULL gave 0.25.'
${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query "
    EXPLAIN SELECT metrics_none.x FROM metrics_none JOIN dim10 ON metrics_none.k = dim10.k" 2>&1 >/dev/null \
    | grep -oE "Estimated statistics for [A-Za-z]+ metrics_none: .*" | head -n 1 | grep -oE '__table1\.x: [0-9]+ \[[^]]*\]'

rm -rf "${LAKE}"
