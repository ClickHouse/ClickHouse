#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Iceberg (USE_AVRO)

# TopN dynamic filtering on an Iceberg table read as `Parquet` that also contains ORC data files.
# Only the Parquet reader applies the filter: an ORC file is read as without TopN, and the results
# are the same with the optimization on and off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05325"
rm -rf "${ICEBERG_DIR}"

# The ORC file has the even keys, the Parquet file the odd ones, so both contribute to every top-K.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t_05325 (k Int64, s String) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'ORC');
    INSERT INTO t_05325 SELECT number * 2, toString(number) FROM numbers(10000);
    INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_DIR}', 'Parquet', 'k Int64, s String')
        SELECT number * 2 + 1, toString(number) FROM numbers(10000);
"

SETTINGS="query_plan_max_limit_for_top_k_optimization = 1000, use_query_condition_cache = 1,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1, enable_analyzer = 1"

for top_k in 1 0; do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT k FROM icebergLocal('${ICEBERG_DIR}', 'Parquet', 'k Int64, s String')
        ORDER BY k DESC LIMIT 4 SETTINGS ${SETTINGS}, use_top_k_dynamic_filtering = ${top_k}" | tr '\n' ' '
    echo
    ${CLICKHOUSE_CLIENT} --query "
        SELECT k, s FROM icebergLocal('${ICEBERG_DIR}', 'Parquet', 'k Int64, s String')
        WHERE k > 100 ORDER BY k LIMIT 4 SETTINGS ${SETTINGS}, use_top_k_dynamic_filtering = ${top_k}" | tr '\n\t' ' :'
    echo
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05325"
rm -rf "${ICEBERG_DIR}"
