#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Resource limits hit in the middle of a write (memory, execution time) must fail closed: no new
# version, no data file left behind, table readable. A many-partition INSERT stays within a bounded
# memory footprint (one open Parquet writer per partition).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_limits_fc"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

bootstrap() {
    local path="$1"
    local partition_cols="$2"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOF
}

state() {
    local path="$1"
    echo "versions: $(($(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1)), data files: $(find "${path}" -name '*.parquet' | wc -l | tr -d ' '), rows: $(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM deltaLakeLocal('${path}')")"
}

error_code() {
    grep -oE '\([A-Z_]+\)' | head -1
}

echo "-- memory limit exceeded while streaming rows (unpartitioned)"
bootstrap "${ROOT}/mem" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/mem')
    SELECT number AS id, repeat('x', 1000) AS s, 0 AS p FROM numbers(3000000)
    SETTINGS max_memory_usage = 50000000, max_block_size = 65536, max_threads = 1;
" 2>&1 < /dev/null | error_code
state "${ROOT}/mem"

echo "-- memory limit exceeded while streaming rows (partitioned)"
bootstrap "${ROOT}/mem_part" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/mem_part')
    SELECT number AS id, repeat('x', 1000) AS s, toInt32(number % 100) AS p FROM numbers(3000000)
    SETTINGS max_memory_usage = 50000000, max_block_size = 65536, max_threads = 1;
" 2>&1 < /dev/null | error_code
state "${ROOT}/mem_part"

echo "-- execution time exceeded while streaming rows"
bootstrap "${ROOT}/timeout" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/timeout')
    SELECT number AS id, 'x' AS s, toInt32(sleepEachRow(0.3) + number % 4) AS p FROM numbers(100)
    SETTINGS max_block_size = 1, max_execution_time = 2, max_threads = 1;
" 2>&1 < /dev/null | error_code
state "${ROOT}/timeout"

echo "-- 1000 partitions in one INSERT: succeeds, one file per partition, bounded memory"
bootstrap "${ROOT}/many" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/many')
    SELECT number AS id, 'x' AS s, toInt32(number % 1000) AS p FROM numbers(2000)
    SETTINGS max_memory_usage = 4000000000, max_threads = 1;
" < /dev/null
state "${ROOT}/many"
echo "partition directories: $(find "${ROOT}/many" -maxdepth 1 -type d -name 'p=*' | wc -l | tr -d ' ')"

echo "-- the table with the failed writes is still intact and writable"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/mem') SELECT 1 AS id, 'ok' AS s, 0 AS p;
    SELECT id, s FROM deltaLakeLocal('${ROOT}/mem');
" < /dev/null
state "${ROOT}/mem"
