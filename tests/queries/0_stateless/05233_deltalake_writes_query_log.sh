#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# `system.query_log` accounting of Delta Lake writes: written rows and bytes of a committed INSERT,
# nothing for a rejected one; an INSERT that fails mid-way commits nothing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_qlog"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

bootstrap() {
    local path="$1"
    local partition_cols="$2"
    mkdir -p "${path}/_delta_log"
    cat > "${path}/_delta_log/00000000000000000000.json" <<EOJ
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-$(basename "${path}")","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"s\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"p\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}]}","partitionColumns":${partition_cols},"configuration":{},"createdTime":1700000000000}}
EOJ
}

versions() {
    echo "versions: $(($(find "$1/_delta_log" -name '*.json' | wc -l | tr -d ' ') - 1))"
}

bootstrap "${ROOT}/plain" '[]'
bootstrap "${ROOT}/part" '["p"]'
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE dl (id Int64, s String, p Int32) ENGINE = DeltaLakeLocal('${ROOT}/plain');
    CREATE TABLE dl_part (id Int64, s String, p Int32) ENGINE = DeltaLakeLocal('${ROOT}/part');
"

RUN=$(random_str 8)
ROWS="SELECT number AS id, toString(number) AS s, toInt32(number % 4) AS p FROM numbers(50000)"

echo "-- committed INSERTs into an unpartitioned and a partitioned table"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query_id="${RUN}_1_plain" --query "INSERT INTO dl ${ROWS}"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query_id="${RUN}_2_part" --query "INSERT INTO dl_part ${ROWS}"
versions "${ROOT}/plain"
versions "${ROOT}/part"

echo "-- rejected INSERT (writes off)"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=0 --query_id="${RUN}_3_rejected" --query "INSERT INTO dl ${ROWS}" 2>&1 | grep -o "SUPPORT_IS_DISABLED" | head -1
versions "${ROOT}/plain"

echo "-- INSERT failing after most rows reached the sink: nothing committed"
$CLICKHOUSE_CLIENT --allow_delta_lake_writes=1 --query_id="${RUN}_4_failed" --query "
    INSERT INTO dl SELECT number AS id, toString(number) AS s, toInt32(throwIf(number = 49999, 'boom') + number % 4) AS p FROM numbers(50000)
    SETTINGS max_block_size = 10000, max_threads = 1
" 2>&1 | grep -o "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" | head -1
versions "${ROOT}/plain"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM dl"

# written_rows counts the rows that reached the sink, so the failed INSERT reports the 40000 rows
# of its four completed blocks although it committed nothing.
echo "-- query_log: type, read rows, written rows, written bytes > 0, inserted rows event"
$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT substring(query_id, 10), type, read_rows, written_rows, written_bytes > 0, ProfileEvents['InsertedRows']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id LIKE '${RUN}\\_%' AND type != 'QueryStart'
    ORDER BY query_id
"

$CLICKHOUSE_CLIENT --query "DROP TABLE dl; DROP TABLE dl_part"
