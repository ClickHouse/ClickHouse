#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function perform()
{
    local test_id=$1
    local query=$2
    local expected=$3
    local is_compressed=$4
    local custom_extension=$5

    local file_extension=".out"
    if [ -n "$custom_extension" ]; then
        file_extension="${file_extension}${custom_extension}"
    fi

    echo "performing test: $test_id"
    ${CLICKHOUSE_CLIENT} --query "$query"
    if [ "$?" -eq 0 ]; then
        local actual
        if [ "$is_compressed" = "1" ]; then
            actual=$(gunzip -c "${CLICKHOUSE_TMP}/test_atomic_${test_id}${file_extension}.gz")
        else
            actual=$(cat "${CLICKHOUSE_TMP}/test_atomic_${test_id}${file_extension}")
        fi
        if [ "$actual" != "$expected" ]; then
            echo "Content mismatch in test_atomic_${test_id}"
            echo "Expected: $expected"
            echo "Got: $actual"
            exit 1
        fi
    else
        echo "query failed"
        exit 1
    fi
    if [ "$is_compressed" = "1" ]; then
        rm -f "${CLICKHOUSE_TMP}/test_atomic_${test_id}${file_extension}.gz"
    else
        rm -f "${CLICKHOUSE_TMP}/test_atomic_${test_id}${file_extension}"
    fi
}

# Test 1: Basic atomic TRUNCATE with TSV
${CLICKHOUSE_CLIENT} --query="SELECT 'old content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_1.out'" || { echo "Failed to create initial file for test 1"; exit 1; }
perform "1" "SELECT 'new content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_1.out' TRUNCATE" "new content" "0"

# Test 2: Atomic TRUNCATE with CSV format
${CLICKHOUSE_CLIENT} --query="SELECT 'old,content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_2.out' FORMAT CSV" || { echo "Failed to create initial file for test 2"; exit 1; }
perform "2" "SELECT 'new,content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_2.out' TRUNCATE FORMAT CSV" "\"new,content\"" "0"

# Test 3: Atomic TRUNCATE with compression
${CLICKHOUSE_CLIENT} --query="SELECT 'old content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_3.out.gz' COMPRESSION 'gzip' FORMAT TSV" || { echo "Failed to create initial file for test 3"; exit 1; }
perform "3" "SELECT 'new content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_3.out.gz' TRUNCATE COMPRESSION 'gzip' FORMAT TSV" "new content" "1"

# Test 4: Raw text file using RawBLOB format
${CLICKHOUSE_CLIENT} --query="SELECT 'old text' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_4.out' FORMAT RawBLOB" || { echo "Failed to create initial file for test 4"; exit 1; }
perform "4" "SELECT 'new text' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_4.out' TRUNCATE FORMAT RawBLOB" "new text" "0"

# Test 5: Multi-line text file using RawBLOB format
${CLICKHOUSE_CLIENT} --query="SELECT 'line1\nline2' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_5.out' FORMAT RawBLOB" || { echo "Failed to create initial file for test 5"; exit 1; }
perform "5" "SELECT 'new line1\nnew line2' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_5.out' TRUNCATE FORMAT RawBLOB" "new line1
new line2" "0"

# Test 6: File with .tmp in its name
${CLICKHOUSE_CLIENT} --query="SELECT 'old content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_6.out.tmp'" || { echo "Failed to create initial file for test 6"; exit 1; }
perform "6" "SELECT 'new content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_6.out.tmp' TRUNCATE" "new content" "0" ".tmp"

echo "OK"