#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/04545_archive_seek_stored_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
PARQUET_FILE="${DATA_DIR}/large.parquet"
ZIP_FILE="${DATA_DIR}/stored.zip"

mkdir -p "$DATA_DIR"

# Large enough STORED zip entry to trigger seeks with leftover buffered data.
"${LOCAL[@]}" -q "
INSERT INTO FUNCTION file('${PARQUET_FILE}', Parquet)
SELECT number AS n, randomPrintableASCII(100) AS s
FROM numbers(200000)
SETTINGS engine_file_truncate_on_insert = 1
"

(
    cd "$DATA_DIR" || exit 1
    zip -0 stored.zip large.parquet >/dev/null
)

# Must read column data (not just metadata) with a small read buffer to exercise seeks.
"${LOCAL[@]}" -q "
SELECT sum(n)
FROM file('${ZIP_FILE} :: large.parquet', Parquet)
SETTINGS max_threads = 4, max_read_buffer_size = 1000
"
