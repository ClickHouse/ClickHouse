#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test: when filtering Iceberg by _file / _path, read_bytes should
# reflect only the matched files, not all files from the manifest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE ${TABLE} (a Int32, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}')
    PARTITION BY (a);
    INSERT INTO ${TABLE} VALUES (1, repeat('x', 10000));
    INSERT INTO ${TABLE} VALUES (2, repeat('y', 10000));
    INSERT INTO ${TABLE} VALUES (3, repeat('z', 10000));
"

# Pick one data file and its path.
ONE_FILE=$(${CLICKHOUSE_CLIENT} --query "SELECT _file FROM ${TABLE} LIMIT 1")
ONE_PATH=$(${CLICKHOUSE_CLIENT} --query "SELECT _path FROM ${TABLE} LIMIT 1")

# Helper: sum all read_bytes from HTTP progress headers.
sum_read_bytes() {
    ${CLICKHOUSE_CURL} -sS \
        "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&default_format=Null" \
        -d @- -v 2>&1 \
        | grep -oP '"read_bytes"\s*:\s*"\K[0-9]+' \
        | awk '{s+=$1} END {print s+0}'
}

BYTES_ALL=$(echo "SELECT * FROM ${TABLE}" | sum_read_bytes)
BYTES_FILE=$(echo "SELECT * FROM ${TABLE} WHERE _file = '${ONE_FILE}'" | sum_read_bytes)
BYTES_PATH=$(echo "SELECT * FROM ${TABLE} WHERE _path = '${ONE_PATH}'" | sum_read_bytes)

# With the fix, filtering to 1 of 3 files should read roughly 1/3 of the
# bytes. Before the fix both values would be equal (all files scanned).
# Use a generous threshold: the filtered value must be less than 60% of the
# total (allows for metadata overhead).
check() {
    local label=$1 filtered=$2 total=$3
    if [ -z "$filtered" ] || [ -z "$total" ] || [ "$total" -eq 0 ] || [ "$filtered" -eq 0 ]; then
        echo "FAIL ($label): could not capture progress (filtered=${filtered:-empty}, total=${total:-empty})"
    elif [ "$((filtered * 100 / total))" -lt 60 ]; then
        echo "OK"
    else
        echo "FAIL ($label): filtered read_bytes (${filtered}) is not significantly less than full scan (${total})"
    fi
}

check "_file" "$BYTES_FILE" "$BYTES_ALL"
check "_path" "$BYTES_PATH" "$BYTES_ALL"
