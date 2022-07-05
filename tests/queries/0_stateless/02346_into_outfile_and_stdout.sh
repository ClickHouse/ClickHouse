#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query" 2>/dev/null
    if [ "$?" -eq 0 ]; then
        cat "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_$test_id.out"
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_$test_id.out"
}

function performFileExists()
{
    local test_id=$1
    local query=$2

    touch "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_$test_id.out"

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query" 2>/dev/null
    if [ "$?" -eq 0 ]; then
        cat "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_$test_id.out"
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_$test_id.out"
}

perform "select" "SELECT 1, 2, 3 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_select.out' AND STDOUT"

perform "bad_query_incorrect_usage" "SELECT 1, 2, 3 INTO OUTFILE AND STDOUT'${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_bad_query_incorrect_usage.out'"

perform "bad_query_no_into_outfile" "SELECT 1, 2, 3 AND STDOUT'"

performFileExists "bad_query_file_exists" "SELECT 1, 2, 3 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_bad_query_file_exists.out' AND STDOUT"