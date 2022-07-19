#!/usr/bin/env bash
# Tags: long, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function query {
    # bash isn't able to store \0 bytes, so use [1; 255] random range
    echo "SELECT greatest(toUInt8(1), toUInt8(intHash64(number))) FROM system.numbers LIMIT $1 FORMAT RowBinary"
}

function ch_url() {
    ${CLICKHOUSE_CURL_COMMAND} -q -sS "${CLICKHOUSE_URL}&max_block_size=$max_block_size&$1" -d "$(query "$2")"
}


# Check correct exceptions handling

exception_pattern="DB::Exception:[[:print:]]*"

function check_only_exception() {
    local res
    res=$(ch_url "$1" "$2")
    #(echo "$res")
    #(echo "$res" | wc -l)
    #(echo "$res" | grep -c "$exception_pattern")
    [[ $(echo "$res" | wc -l) -eq 1 ]] || echo FAIL 1 "$@"
    [[ $(echo "$res" | grep -c "$exception_pattern") -eq 1 ]] || echo FAIL 2 "$@"
}

function check_last_line_exception() {
    local res
    res=$(ch_url "$1" "$2")
    #echo "$res" > res
    #echo "$res" | wc -c
    #echo "$res" | tail -n -2
    [[ $(echo "$res" | tail -n -1 | grep -c "$exception_pattern") -eq 1 ]] || echo FAIL 3 "$@"
    [[ $(echo "$res" | head -n -1 | grep -c "$exception_pattern") -eq 0 ]] || echo FAIL 4 "$@"
}

function check_exception_handling() {
    check_only_exception "max_result_bytes=1000"                        1001
    check_only_exception "max_result_bytes=1000&wait_end_of_query=1"    1001

    check_only_exception "max_result_bytes=1048576&buffer_size=1048576&wait_end_of_query=0" 1048577
    check_only_exception "max_result_bytes=1048576&buffer_size=1048576&wait_end_of_query=1" 1048577

    check_only_exception "max_result_bytes=1500000&buffer_size=2500000&wait_end_of_query=0" 1500001
    check_only_exception "max_result_bytes=1500000&buffer_size=1500000&wait_end_of_query=1" 1500001

    check_only_exception         "max_result_bytes=4000000&buffer_size=2000000&wait_end_of_query=1" 5000000
    check_only_exception         "max_result_bytes=4000000&wait_end_of_query=1" 5000000
    check_last_line_exception    "max_result_bytes=4000000&buffer_size=2000000&wait_end_of_query=0" 5000000
}

check_exception_handling


# Tune setting to speed up combinatorial test
max_block_size=500000
corner_sizes="1048576 $(seq 500000 1000000 3500000)"


# Check HTTP results with $CLICKHOUSE_CLIENT in normal case

function cmp_cli_and_http() {
    $CLICKHOUSE_CLIENT -q "$(query "$1")" > "${CLICKHOUSE_TMP}"/res1
    ch_url "buffer_size=$2&wait_end_of_query=0" "$1" > "${CLICKHOUSE_TMP}"/res2
    ch_url "buffer_size=$2&wait_end_of_query=1" "$1" > "${CLICKHOUSE_TMP}"/res3
    cmp "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res2 && cmp "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res3 || echo FAIL 5 "$@"
    rm -rf "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res2 "${CLICKHOUSE_TMP}"/res3
}

function check_cli_and_http() {
    for input_size in $corner_sizes; do
        for buffer_size in $corner_sizes; do
            #echo "$input_size" "$buffer_size"
            cmp_cli_and_http "$input_size" "$buffer_size"
        done
    done
}

check_cli_and_http


# Check HTTP internal compression in normal case

function cmp_http_compression() {
    $CLICKHOUSE_CLIENT -q "$(query "$1")" > "${CLICKHOUSE_TMP}"/res0
    ch_url 'compress=1' "$1" | "${CLICKHOUSE_BINARY}"-compressor --decompress > "${CLICKHOUSE_TMP}"/res1
    ch_url "compress=1&buffer_size=$2&wait_end_of_query=0" "$1" | "${CLICKHOUSE_BINARY}"-compressor --decompress > "${CLICKHOUSE_TMP}"/res2
    ch_url "compress=1&buffer_size=$2&wait_end_of_query=1" "$1" | "${CLICKHOUSE_BINARY}"-compressor --decompress > "${CLICKHOUSE_TMP}"/res3
    cmp "${CLICKHOUSE_TMP}"/res0 "${CLICKHOUSE_TMP}"/res1
    cmp "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res2
    cmp "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res3
    rm -rf "${CLICKHOUSE_TMP}"/res0 "${CLICKHOUSE_TMP}"/res1 "${CLICKHOUSE_TMP}"/res2 "${CLICKHOUSE_TMP}"/res3
}

function check_http_compression() {
    for input_size in $corner_sizes; do
        for buffer_size in $corner_sizes; do
            #echo "$input_size" "$buffer_size"
            cmp_http_compression "$input_size" "$buffer_size"
        done
    done
}

check_http_compression
