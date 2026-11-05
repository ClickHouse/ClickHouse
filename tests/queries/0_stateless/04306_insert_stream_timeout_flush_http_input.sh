#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check
# no-fasttest: The test waits for a timeout-based flush from an open HTTP request.
# no-parallel: The partial flush is timing-sensitive; concurrent load can starve the pipeline executor.
# no-flaky-check: The test verifies a timeout-based behavior and is not suitable for rerun-based flakiness detection.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIRECT="test_http_stream_timeout_direct"
TABLE_INPUT="test_http_stream_timeout_input"
TABLE_COMPRESSED="test_http_stream_timeout_compressed"
TMP_PREFIX="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
FIRST_BATCH_ROWS=1024
SECOND_BATCH_ROWS=4
LINE_PAYLOAD="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

cleanup()
{
    jobs -pr | xargs -r kill 2>/dev/null || true
    rm -f "${TMP_PREFIX}"_*.data "${TMP_PREFIX}"_*.data.lz4
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_DIRECT}" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_INPUT}" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_COMPRESSED}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_DIRECT}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_INPUT}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE_COMPRESSED}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_DIRECT} (line String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_INPUT} (line String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_COMPRESSED} (line String) ENGINE = MergeTree ORDER BY tuple()"

run_case()
{
    local label=$1
    local table=$2
    local query=$3
    local compressed=${4:-0}

    local first_batch="${TMP_PREFIX}_${label}_first.data"
    local second_batch="${TMP_PREFIX}_${label}_second.data"

    : > "$first_batch"
    : > "$second_batch"

    for i in $(seq 1 "$FIRST_BATCH_ROWS"); do
        printf '%s_first_%05d_%s\n' "$label" "$i" "$LINE_PAYLOAD" >> "$first_batch"
    done
    for i in $(seq 1 "$SECOND_BATCH_ROWS"); do
        printf '%s_second_%05d_%s\n' "$label" "$i" "$LINE_PAYLOAD" >> "$second_batch"
    done

    if [[ "$compressed" == "1" ]]; then
        local first_batch_compressed="${TMP_PREFIX}_${label}_first.data.lz4"
        local second_batch_compressed="${TMP_PREFIX}_${label}_second.data.lz4"
        ${CLICKHOUSE_COMPRESSOR} < "$first_batch" > "$first_batch_compressed"
        ${CLICKHOUSE_COMPRESSOR} < "$second_batch" > "$second_batch_compressed"
        first_batch="$first_batch_compressed"
        second_batch="$second_batch_compressed"
    fi

    local extra_params=""
    if [[ "$compressed" == "1" ]]; then
        extra_params="&decompress=1"
    fi

    local insert_url="${CLICKHOUSE_URL}&async_insert=0&min_insert_block_size_rows=0&min_insert_block_size_bytes=0&input_format_max_block_wait_ms=500&input_format_connection_handling=1${extra_params}&query=${query}"
    local url_without_scheme="${insert_url#http://}"
    if [[ "$url_without_scheme" == "$insert_url" ]]; then
        echo "Only HTTP URLs are supported by this test"
        exit 1
    fi

    local host_port="${url_without_scheme%%/*}"
    local request_path="/${url_without_scheme#*/}"
    local host="${host_port%%:*}"
    local port=80
    if [[ "$host_port" == *:* ]]; then
        port="${host_port##*:}"
    fi

    local http_fd
    exec {http_fd}<>/dev/tcp/"$host"/"$port"
    printf 'POST %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: clickhouse-test\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n' \
        "$request_path" "$host_port" >&${http_fd}

    send_chunk()
    {
        local chunk_file=$1
        local chunk_size
        chunk_size=$(wc -c < "$chunk_file")
        printf '%x\r\n' "$chunk_size" >&${http_fd}
        cat "$chunk_file" >&${http_fd}
        printf '\r\n' >&${http_fd}
    }

    send_chunk "$first_batch"

    # Poll for the partial flush. Use wall-clock time so that slow debug builds
    # under CI contention still see the timeout-based flush before giving up.
    local deadline=$(($(date +%s) + 120))
    local rows_before_close=0
    while (( $(date +%s) < deadline )); do
        rows_before_close=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}")
        if [[ "$rows_before_close" == "$FIRST_BATCH_ROWS" ]]; then
            break
        fi
        sleep 0.25
    done

    echo "${label}: rows before close: ${rows_before_close}"

    send_chunk "$second_batch"
    printf '0\r\n\r\n' >&${http_fd}

    local status_line
    if ! IFS= read -r -t 30 status_line <&${http_fd}; then
        echo "${label}: no HTTP response"
        exec {http_fd}>&-
        exit 1
    fi

    exec {http_fd}>&-

    if [[ "$status_line" != HTTP/*" 200 "* ]]; then
        echo "${label}: unexpected HTTP response: ${status_line}"
        exit 1
    fi

    echo "${label}: final rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table}")"
}

run_case \
    direct \
    "$TABLE_DIRECT" \
    "INSERT%20INTO%20${TABLE_DIRECT}%20FORMAT%20LineAsString"

run_case \
    input \
    "$TABLE_INPUT" \
    "INSERT%20INTO%20${TABLE_INPUT}%20SELECT%20line%20FROM%20input%28%27line%20String%27%29%20FORMAT%20LineAsString"

run_case \
    compressed \
    "$TABLE_COMPRESSED" \
    "INSERT%20INTO%20${TABLE_COMPRESSED}%20FORMAT%20LineAsString" \
    1
