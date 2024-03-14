#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2

    echo "performing test: ${test_id}"
    ${CLICKHOUSE_CLIENT} --query "${query}" | sort --numeric-sort
    if [ "$?" -eq 0 ]; then
        cat "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}.out" | sort --numeric-sort
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}.out"
}

function performBadQuery()
{
    local test_id=$1
    local query=$2
    local error_message=$3

    echo "performing test: ${test_id}"
    ${CLICKHOUSE_CLIENT} --query "${query}" 2>&1 |  grep -Fc "${error_message}"
}

function performFileExists()
{
    local test_id=$1
    local query=$2
    local error_message=$3

    touch "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}.out"

    echo "performing test: ${test_id}"
    ${CLICKHOUSE_CLIENT} --query "${query}" 2>&1 |  grep -Fc "${error_message}"
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}.out"
}

function performCompression()
{
    local test_id=$1
    local query=$2

    echo "performing test: ${test_id}"
    ${CLICKHOUSE_CLIENT} --query "${query}"
    if [ "$?" -eq 0 ]; then
        gunzip "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}.gz"
        cat "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}"
    else
        echo "query failed"
    fi
    rm -f "${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_${test_id}"
}



perform "select" "SELECT 1, 2, 3 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_select.out' AND STDOUT"

performBadQuery "bad_query_incorrect_usage" "SELECT 1, 2, 3 INTO OUTFILE AND STDOUT'${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_bad_query_incorrect_usage.out'" "SYNTAX_ERROR"

performBadQuery "bad_query_no_into_outfile" "SELECT 1, 2, 3 AND STDOUT'" "SYNTAX_ERROR"

performFileExists "bad_query_file_exists" "SELECT 1, 2, 3 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_bad_query_file_exists.out' AND STDOUT" "File exists. (CANNOT_OPEN_FILE)"

performCompression "compression" "SELECT * FROM (SELECT 'Hello, World! From clickhouse.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_compression.gz' AND STDOUT COMPRESSION 'GZ' FORMAT TabSeparated"

performBadQuery "bad_query_misplaced_compression" "SELECT 1, 2, 3 INTO OUTFILE 'test.gz' COMPRESSION 'GZ' AND STDOUT'" "SYNTAX_ERROR"

performBadQuery "bad_query_misplaced_format" "SELECT 1, 2, 3 INTO OUTFILE 'test.gz' FORMAT TabSeparated AND STDOUT'" "SYNTAX_ERROR"

perform "union_all" "SELECT 3, 4 UNION ALL SELECT 1, 2 INTO OUTFILE '${CLICKHOUSE_TMP}/test_into_outfile_and_stdout_union_all.out' AND STDOUT"