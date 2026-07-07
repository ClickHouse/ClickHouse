#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When object_storage_fsync_after_insert = 1, a data file written by an object
# storage sink must be fdatasync'd (FileSync profile event) before the insert
# completes, so committed data is durable across a hard failure. When the
# setting is off, no fsync happens (previous behavior). Exercised via the
# DeltaLakeLocal write path, which is where a lost data-file tail can leave a
# committed-but-truncated parquet file (issue #109664).

TABLE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_fsync_delta"

make_empty_delta_table() {
    rm -rf "${TABLE_DIR}"
    mkdir -p "${TABLE_DIR}/_delta_log"
    cat > "${TABLE_DIR}/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"b\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}
EOF
}

# Count the FileSync profile events emitted while running one insert.
run_insert() {
    local fsync="$1"
    ${CLICKHOUSE_LOCAL} --print-profile-events --profile-events-delay-ms=-1 -q "
        SET allow_experimental_delta_lake_writes = 1, object_storage_fsync_after_insert = ${fsync};
        INSERT INTO TABLE FUNCTION deltaLakeLocal('${TABLE_DIR}')
            SELECT number AS a, toString(number) AS b FROM numbers(50);
    " 2>&1 | grep -c -E '\bFileSync:'
}

make_empty_delta_table
off=$(run_insert 0)
make_empty_delta_table
on=$(run_insert 1)

# off: no fdatasync; on: at least one fdatasync of the written data file.
[ "${off}" -eq 0 ] && echo "fsync_off: no FileSync" || echo "fsync_off: unexpected FileSync (${off})"
[ "${on}" -ge 1 ] && echo "fsync_on: FileSync fired" || echo "fsync_on: FileSync missing"

# Data is still readable after a durable insert.
${CLICKHOUSE_LOCAL} -q "SET allow_experimental_delta_lake_writes = 1; SELECT count() FROM deltaLakeLocal('${TABLE_DIR}')"

rm -rf "${TABLE_DIR}"
