#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# `delta_lake_insert_max_rows_in_data_file` / `delta_lake_insert_max_bytes_in_data_file` split one INSERT
# into several files of one commit; with 10000-row chunks the split is deterministic.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_limits"
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

# Row count of every data file the latest commit added, sorted.
rows_per_file() {
    local path="$1"
    local latest
    latest=$(find "${path}/_delta_log" -name '*.json' | sort | tail -1)
    ${CLICKHOUSE_LOCAL} --query "
        SELECT decodeURLComponent(JSONExtractString(line, 'add', 'path'))
        FROM file('${latest}', LineAsString)
        WHERE JSONHas(line, 'add')
    " | while read -r data_file; do
        ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${path}/${data_file}', Parquet)"
    done | sort -n | tr '\n' ',' | sed 's/,$//'
}

report() {
    local path="$1"
    echo "versions: $(($(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
    local rows
    rows=$(rows_per_file "${path}")
    echo "data files in latest commit: $(echo "${rows}" | tr ',' '\n' | wc -l | tr -d ' ')"
    echo "rows per file: ${rows}"
    echo "readable rows: $(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM deltaLakeLocal('${path}')")"
}

bytes_report() {
    local path="$1"
    local rows
    rows=$(rows_per_file "${path}")
    ${CLICKHOUSE_LOCAL} --query "
        WITH arrayMap(x -> toUInt64(x), splitByChar(',', '${rows}')) AS rows
        SELECT
            'versions: ' || toString($(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1),
            'more than one data file: ' || toString(length(rows) > 1),
            'no file exceeds the limit by more than one chunk: ' || toString(arrayMax(rows) <= 30000),
            'all rows committed: ' || toString(arraySum(rows) = 200000)
        FORMAT TSVRaw
    " | tr '\t' '\n'
    echo "readable rows: $(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM deltaLakeLocal('${path}')")"
}

# 10000-row chunks reach the sink.
CHUNKS="max_block_size = 10000, min_insert_block_size_rows = 10000, min_insert_block_size_bytes = 0, max_threads = 1, max_insert_threads = 1"
INSERT_200K="SELECT number AS id, repeat('x', 100) AS s, toInt32(number % 2) AS p FROM numbers(200000)"

echo "-- rows limit 50000 on an unpartitioned table: 4 files of exactly 50000 rows, one commit"
bootstrap "${ROOT}/rows" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/rows') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_rows_in_data_file = 50000, ${CHUNKS};
"
report "${ROOT}/rows"

echo "-- rows limit 50000 on a table partitioned by p: the limit applies per partition"
bootstrap "${ROOT}/rows_part" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/rows_part') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_rows_in_data_file = 50000, ${CHUNKS};
"
report "${ROOT}/rows_part"
echo "partition directories: $(find "${ROOT}/rows_part" -maxdepth 1 -type d -name 'p=*' | wc -l | tr -d ' ')"

echo "-- bytes limit rotates too: a 200k-row INSERT of ~110-byte rows must not fit one 2 MB file,"
echo "-- and no file may exceed the limit by more than one 10000-row chunk"
bootstrap "${ROOT}/bytes" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bytes') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_bytes_in_data_file = 2000000, ${CHUNKS};
"
bytes_report "${ROOT}/bytes"

echo "-- bytes limit on a table partitioned by p: the rotation happens per partition too"
bootstrap "${ROOT}/bytes_part" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bytes_part') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_bytes_in_data_file = 2000000, ${CHUNKS};
"
bytes_report "${ROOT}/bytes_part"
# Per-partition accounting: every partition gets 5000 rows of each 10000-row chunk, so a partition rotates
# its file after the 4th chunk (20000 rows of ~117 bytes exceed 2 MB). A budget shared across the two
# partitions would rotate after 2 chunks and leave 10000-row files instead.
for d in "${ROOT}"/bytes_part/p=*; do
    echo "$(basename "${d}"): rows per file: $(find "${d}" -name '*.parquet' | while read -r f; do ${CLICKHOUSE_LOCAL} --query "SELECT count() FROM file('${f}', Parquet)" < /dev/null; done | sort -n | tr '\n' ',' | sed 's/,$//')"
done

echo "-- default limits (1M rows, 1 GiB): the same INSERT stays in a single file"
bootstrap "${ROOT}/defaults" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/defaults') ${INSERT_200K} SETTINGS ${CHUNKS};
"
report "${ROOT}/defaults"

echo "-- both limits set: the tighter one wins (rows 50000 vs bytes 100 MB)"
bootstrap "${ROOT}/both" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_rows_in_data_file = 50000, delta_lake_insert_max_bytes_in_data_file = 100000000, ${CHUNKS};
"
report "${ROOT}/both"

echo "-- a zero limit is rejected up front"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') SELECT 1 AS id, 'x' AS s, 0 AS p SETTINGS delta_lake_insert_max_rows_in_data_file = 0;
" 2>&1 | grep -oE '\([A-Z_]+\)' | head -1
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') SELECT 1 AS id, 'x' AS s, 0 AS p SETTINGS delta_lake_insert_max_bytes_in_data_file = 0;
" 2>&1 | grep -oE '\([A-Z_]+\)' | head -1
