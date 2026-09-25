#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: needs Parquet and Delta Lake
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `deltaLakeLocal` is absent.

# TopN dynamic filtering (`ORDER BY <column> LIMIT n`) must not apply to a Delta Lake partition column:
# results with and without the optimization must agree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="query_plan_max_limit_for_top_k_optimization = 1000, input_format_parquet_use_native_reader_v3 = 1,
    input_format_parquet_filter_push_down = 1, max_threads = 1, max_parsing_threads = 1, use_query_condition_cache = 0"
ON="use_top_k_dynamic_filtering = 1"
OFF="use_top_k_dynamic_filtering = 0"

# The legacy Delta Lake reader (`allow_delta_kernel_rs = 0`) replaces the partition columns of the chunks
# the format returns with the partition values. These data files store the partition column as well,
# with other values (the file of `p = 1` stores 100, the one of `p = 2` stores 0, and it is read first):
# comparing them against the threshold would drop the rows of `p = 1`.
DELTA_DIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_05257"
rm -rf "${DELTA_DIR}"
mkdir -p "${DELTA_DIR}/_delta_log"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DELTA_DIR}/a/data.parquet', Parquet, 'id Int32, p Int32') SELECT number, 0 FROM numbers(1000);
    INSERT INTO FUNCTION file('${DELTA_DIR}/z/data.parquet', Parquet, 'id Int32, p Int32') SELECT number, 100 FROM numbers(1000);
"
DELTA_SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}'
cat > "${DELTA_DIR}/_delta_log/00000000000000000000.json" <<LOG
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}_05257","format":{"provider":"parquet","options":{}},"schemaString":"${DELTA_SCHEMA}","partitionColumns":["p"],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"a/data.parquet","partitionValues":{"p":"2"},"size":1,"modificationTime":1700000000000,"dataChange":true}}
{"add":{"path":"z/data.parquet","partitionValues":{"p":"1"},"size":1,"modificationTime":1700000000000,"dataChange":true}}
LOG
for kernel in 0 1
do
    DELTA="deltaLakeLocal('${DELTA_DIR}', 'Parquet', 'id Int32, p Int32')"
    diff <(${CLICKHOUSE_CLIENT} --allow_delta_kernel_rs "${kernel}" --query "SELECT p, id FROM ${DELTA} ORDER BY p, id LIMIT 3 SETTINGS ${SETTINGS}, ${ON}") \
         <(${CLICKHOUSE_CLIENT} --allow_delta_kernel_rs "${kernel}" --query "SELECT p, id FROM ${DELTA} ORDER BY p, id LIMIT 3 SETTINGS ${SETTINGS}, ${OFF}") \
        && echo "allow_delta_kernel_rs = ${kernel}: OK"
done
rm -rf "${DELTA_DIR}"
