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

# One clickhouse-local process per report (a process start is the expensive part under sanitizers):
# every table here receives exactly one INSERT, so every data file under the table belongs to the
# latest commit. Row counts come from the files themselves, per file and (for partitioned tables)
# per partition directory.
report() {
    local path="$1"
    local partitioned="$2"   # 0 or 1
    echo "versions: $(($(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
    ${CLICKHOUSE_LOCAL} --query "
        CREATE TEMPORARY TABLE files AS
            SELECT _path AS file, splitByChar('/', _path)[-2] AS dir, count() AS rows
            FROM file('${path}/**/*.parquet', Parquet) GROUP BY file;
        SELECT 'data files in latest commit: ' || toString(count()) FROM files;
        SELECT 'rows per file: ' || arrayStringConcat(arrayMap(x -> toString(x), arraySort(groupArray(rows))), ',') FROM files;
        SELECT 'readable rows: ' || toString(count()) FROM deltaLakeLocal('${path}');
        SELECT dir || ': rows per file: ' || arrayStringConcat(arrayMap(x -> toString(x), arraySort(groupArray(rows))), ',')
        FROM files WHERE ${partitioned} GROUP BY dir ORDER BY dir;
    " --output-format TSVRaw
}

# The bytes limit counts in-memory bytes, so only bounds are pinned for the unpartitioned case.
bytes_report() {
    local path="$1"
    echo "versions: $(($(find "${path}/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
    ${CLICKHOUSE_LOCAL} --query "
        CREATE TEMPORARY TABLE files AS
            SELECT _path AS file, count() AS rows FROM file('${path}/**/*.parquet', Parquet) GROUP BY file;
        SELECT 'more than one data file: ' || toString(count() > 1) FROM files;
        SELECT 'no file exceeds the limit by more than one chunk: ' || toString(max(rows) <= 30000) FROM files;
        SELECT 'all rows committed: ' || toString(sum(rows) = 200000) FROM files;
        SELECT 'readable rows: ' || toString(count()) FROM deltaLakeLocal('${path}');
    " --output-format TSVRaw
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
report "${ROOT}/rows" 0

echo "-- rows limit 50000 on a table partitioned by p: the limit applies per partition"
bootstrap "${ROOT}/rows_part" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/rows_part') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_rows_in_data_file = 50000, ${CHUNKS};
"
report "${ROOT}/rows_part" 1

echo "-- bytes limit rotates too: a 200k-row INSERT of ~110-byte rows must not fit one 2 MB file,"
echo "-- and no file may exceed the limit by more than one 10000-row chunk"
bootstrap "${ROOT}/bytes" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bytes') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_bytes_in_data_file = 2000000, ${CHUNKS};
"
bytes_report "${ROOT}/bytes"

echo "-- bytes limit on a table partitioned by p: the budget is per partition. Every partition gets 5000 rows"
echo "-- of each 10000-row chunk (~117 bytes each), so it rotates after the 4th chunk: 20000-row files."
echo "-- A budget shared across the two partitions would rotate after 2 chunks and give 10000-row files."
bootstrap "${ROOT}/bytes_part" '["p"]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/bytes_part') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_bytes_in_data_file = 2000000, ${CHUNKS};
"
report "${ROOT}/bytes_part" 1

echo "-- default limits (1M rows, 1 GiB): the same INSERT stays in a single file"
bootstrap "${ROOT}/defaults" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/defaults') ${INSERT_200K} SETTINGS ${CHUNKS};
"
report "${ROOT}/defaults" 0

echo "-- both limits set: the tighter one wins (rows 50000 vs bytes 100 MB)"
bootstrap "${ROOT}/both" '[]'
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') ${INSERT_200K}
    SETTINGS delta_lake_insert_max_rows_in_data_file = 50000, delta_lake_insert_max_bytes_in_data_file = 100000000, ${CHUNKS};
"
report "${ROOT}/both" 0

echo "-- a zero limit is rejected up front"
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') SELECT 1 AS id, 'x' AS s, 0 AS p SETTINGS delta_lake_insert_max_rows_in_data_file = 0;
" 2>&1 | grep -oE '\([A-Z_]+\)' | head -1
${CLICKHOUSE_LOCAL} --allow_delta_lake_writes=1 --query "
    INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/both') SELECT 1 AS id, 'x' AS s, 0 AS p SETTINGS delta_lake_insert_max_bytes_in_data_file = 0;
" 2>&1 | grep -oE '\([A-Z_]+\)' | head -1
