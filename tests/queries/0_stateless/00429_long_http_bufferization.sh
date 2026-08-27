#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

format="RowBinary"

function query() {
    # bash isn't able to store \0 bytes, so use [1; 255] random range
    echo "SELECT greatest(toUInt8(1), toUInt8(intHash64(number))) FROM system.numbers LIMIT $1 FORMAT $format"
}

function ch_url() {
    ${CLICKHOUSE_CURL_COMMAND} -q -sS \
        "${CLICKHOUSE_URL}${max_block_size:+"&max_block_size=$max_block_size"}&$1" \
        -d "$(query "$2")"
}

function ch_url_safe() {
    read -r retval stdout_tmp stderr_tmp <<< "$(run_with_error ch_url """$@""")"

    local out=""
    out=$(cat "${stdout_tmp}")
    rm -rf "${stdout_tmp}"
    local err=""
    err=$(cat "${stderr_tmp}")
    rm -rf "${stderr_tmp}"

    # echo "ch_url_safe out <${out}>" 1>&2
    # echo "ch_url_safe err <${err}>" 1>&2

    case "${retval}" in
        "0")
            echo -n "$out"
            ;;
        "18")
            if [[ "${err}" = 'curl: (18) transfer closed with outstanding read data remaining' ]]
            then
                echo -n "$out"
            else
                echo -n "$err"
                return 2
            fi
            ;;
        *)
            echo -n "$err";
            return 1
            ;;
    esac
}

# Check correct exceptions handling

exception_pattern="DB::Exception:[[:print:]]*"
exception_mark="__exception__"

function check_only_exception() {
    local res
    res=$(ch_url_safe "$@")
    # echo "$res"
    # echo -n "wc -l:"; echo "$res" | wc -l
    # echo "$res" | grep -c "$exception_pattern"
    [[ $(echo "$res" | wc -l) -eq 1 ]] || echo FAIL 1 "$@"
    [[ $(echo "$res" | grep -c "$exception_pattern") -eq 1 ]] || echo FAIL 2 "$@"
}

function check_last_line_exception() {
    local res
    res=$(ch_url_safe "$@")
    # echo "$res"
    # echo -n "wc -c:"; echo "$res" | wc -c
    # echo -n "tail -n 3:"; echo "$res" | tail -n 3
    if [[ $(echo "$res" | wc -l) -gt 1 ]]
    then
        [[ $(echo "$res" | head -n 1 | grep -c "$exception_mark") -eq 0 ]] || echo FAIL 3 "$@" "<${res}>"
        [[ $(echo "$res" | tail -n 2 | head -n 1  | grep -c "$exception_mark") -eq 1 ]] || echo -n FAIL 4 "$@" "<${res}>"
    fi

    [[ $(echo "$res" | tail -n 1 | grep -c "$exception_pattern") -eq 1 ]] || echo FAIL 5 "$@" "<${res}>"
}

function check_exception_handling() {
    format=TSV \
    check_last_line_exception \
        "max_block_size=30000&max_result_rows=400000&http_response_buffer_size=1048577&http_wait_end_of_query=0" 111222333444

    check_only_exception "max_result_bytes=1000"                        1001

    check_only_exception "max_result_bytes=1000&http_wait_end_of_query=1"    1001

    check_last_line_exception "max_result_bytes=1048576&http_response_buffer_size=1048576&http_wait_end_of_query=0" 1048577
    check_only_exception      "max_result_bytes=1048576&http_response_buffer_size=1048576&http_wait_end_of_query=1" 1048577

    check_only_exception "max_result_bytes=1500000&http_response_buffer_size=2500000&http_wait_end_of_query=0" 1500001
    check_only_exception "max_result_bytes=1500000&http_response_buffer_size=1500000&http_wait_end_of_query=1" 1500001

    check_only_exception         "max_result_bytes=4000000&http_response_buffer_size=2000000&http_wait_end_of_query=1" 5000000
    check_only_exception         "max_result_bytes=4000000&http_wait_end_of_query=1" 5000000
    check_last_line_exception    "max_result_bytes=4000000&http_response_buffer_size=2000000&http_wait_end_of_query=0" 5000000
}

check_exception_handling


# Tune setting to speed up combinatorial test
max_block_size=500000
corner_sizes="1048576 $(seq 500000 1000000 3500000)"


# Check HTTP results with $CLICKHOUSE_CLIENT in normal case

function cmp_cli_and_http() {
    $CLICKHOUSE_CLIENT -q "$(query "$1")" > "${CLICKHOUSE_TMP}"/res1
    ch_url "http_response_buffer_size=$2&http_wait_end_of_query=0" "$1" > "${CLICKHOUSE_TMP}"/res2
    ch_url "http_response_buffer_size=$2&http_wait_end_of_query=1" "$1" > "${CLICKHOUSE_TMP}"/res3
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
    ch_url 'compress=1' "$1" | "${CLICKHOUSE_COMPRESSOR}" --decompress > "${CLICKHOUSE_TMP}"/res1
    ch_url "compress=1&http_response_buffer_size=$2&http_wait_end_of_query=0" "$1" | "${CLICKHOUSE_COMPRESSOR}" --decompress > "${CLICKHOUSE_TMP}"/res2
    ch_url "compress=1&http_response_buffer_size=$2&http_wait_end_of_query=1" "$1" | "${CLICKHOUSE_COMPRESSOR}" --decompress > "${CLICKHOUSE_TMP}"/res3
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
