#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The legacy (non delta-kernel) DeltaLake reader inserts partition columns into the chunk with the
# type parsed from `_delta_log`, which is not the declared column type when the table function (or
# the insertion table, through `use_structure_from_insertion_table_in_table_functions`) asks for a
# different one. Here `_delta_log` says `Nullable(DateTime64(6))` and the query asks for `DateTime`.
# Without a conversion, a query with no filter reads the column through the wrong type and returns
# garbage, and a filter on it throws a `LOGICAL_ERROR` out of the expression it is compared with.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_legacy_partition"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"
mkdir -p "${ROOT}/_delta_log"

# Data file holding only the non-partition column, as a partitioned Delta table stores it.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${ROOT}/process_time=2026-09-21 09%3A00%3A00/data.parquet', Parquet, 'id Int32') VALUES (1);
" < /dev/null

# A v0 transaction log declaring `process_time` as a nullable `timestamp` partition column.
SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"process_time\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}}]}'
cat > "${ROOT}/_delta_log/00000000000000000000.json" <<LOG
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}","format":{"provider":"parquet","options":{}},"schemaString":"${SCHEMA}","partitionColumns":["process_time"],"configuration":{},"createdTime":1700000000000}}
{"add":{"path":"process_time=2026-09-21%2009%253A00%253A00/data.parquet","partitionValues":{"process_time":"2026-09-21 09:00:00"},"size":1,"modificationTime":1700000000000,"dataChange":true}}
LOG

# `id Int32, process_time DateTime` is what an `INSERT INTO <table> SELECT * FROM deltaLake(...)`
# passes down when the destination declares `process_time` as `DateTime`.
TABLE_FUNCTION="deltaLakeLocal('${ROOT}', 'Parquet', 'id Int32, process_time DateTime')"
NULLABLE_TABLE_FUNCTION="deltaLakeLocal('${ROOT}', 'Parquet', 'id Int32, process_time Nullable(DateTime)')"

for kernel in 0 1
do
    echo "-- allow_delta_kernel_rs = ${kernel}"
    # `session_timezone` is pinned because the two readers disagree on the zone a partition
    # timestamp is parsed in, which is a separate question from the type this test is about.
    ${CLICKHOUSE_CLIENT} --allow_experimental_delta_kernel_rs="${kernel}" --session_timezone UTC --query "
        SELECT toTypeName(process_time), * FROM ${TABLE_FUNCTION};
        SELECT * FROM ${TABLE_FUNCTION} WHERE process_time = '2026-09-21 09:00:00';
        SELECT * FROM ${TABLE_FUNCTION} WHERE toDateTime(process_time) = toDateTime('2026-09-21 09:00:00');
        SELECT * FROM ${TABLE_FUNCTION} WHERE process_time = '2026-09-21 10:00:00';
        SELECT toTypeName(process_time), * FROM ${NULLABLE_TABLE_FUNCTION};
    " < /dev/null
done
