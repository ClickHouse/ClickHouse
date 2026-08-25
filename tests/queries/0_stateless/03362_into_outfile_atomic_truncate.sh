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
${CLICKHOUSE_CLIENT} --query="SELECT 'old content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_3.out.gz' FORMAT TSV" || { echo "Failed to create initial file for test 3"; exit 1; }
perform "3" "SELECT 'new content' INTO OUTFILE '${CLICKHOUSE_TMP}/test_atomic_3.out.gz' TRUNCATE FORMAT TSV" "new content" "1"

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

# Test 7: Original file is untouched during query execution (atomicity)
echo "performing test: 7"
OUT_FILE_7="${CLICKHOUSE_TMP}/test_atomic_7.out"
rm -f "${OUT_FILE_7}"
${CLICKHOUSE_CLIENT} --query="SELECT 'old_content' INTO OUTFILE '${OUT_FILE_7}' FORMAT RawBLOB" || { echo "Failed to create initial file for test 7"; exit 1; }
# Run a slow query in the background so we can inspect files mid-execution.
${CLICKHOUSE_CLIENT} --query="SELECT sleepEachRow(0.5), number FROM numbers(6) INTO OUTFILE '${OUT_FILE_7}' TRUNCATE FORMAT TSV" &
BG_PID=$!
# Wait until the temp file appears (up to 5 seconds).
for _ in $(seq 1 50); do
    TEMP_FILE=$(ls "${CLICKHOUSE_TMP}"/tmp_*.test_atomic_7.out 2>/dev/null | head -1)
    if [ -n "$TEMP_FILE" ]; then
        break
    fi
    sleep 0.1
done
if [ -z "$TEMP_FILE" ]; then
    echo "temp file never appeared for test 7"
    wait "$BG_PID" 2>/dev/null
    exit 1
fi
# While the query is running, the original file must still have old content.
mid_content=$(cat "${OUT_FILE_7}" 2>/dev/null)
if [ "$mid_content" != "old_content" ]; then
    echo "Original file modified during query in test 7"
    echo "Expected: old_content"
    echo "Got: $mid_content"
    wait "$BG_PID" 2>/dev/null
    exit 1
fi
wait "$BG_PID" 2>/dev/null
# After completion, the original file has new data and the temp file is gone.
if [ -e "$TEMP_FILE" ]; then
    echo "Temp file not cleaned up after query in test 7"
    exit 1
fi
new_content=$(head -1 "${OUT_FILE_7}")
if [ "$new_content" != "0	0" ]; then
    echo "Final content mismatch in test 7"
    echo "Expected first line: 0	0"
    echo "Got: $new_content"
    exit 1
fi
rm -f "${OUT_FILE_7}"

# Test 8: Error during query preserves original file content
echo "performing test: 8"
OUT_FILE_8="${CLICKHOUSE_TMP}/test_atomic_8.out"
rm -f "${OUT_FILE_8}"
${CLICKHOUSE_CLIENT} --query="SELECT 'old_content' INTO OUTFILE '${OUT_FILE_8}' FORMAT RawBLOB" || { echo "Failed to create initial file for test 8"; exit 1; }
# This query will fail mid-execution: the first block (number=0) succeeds,
# but throwIf triggers on number>=1. The original file should be preserved.
${CLICKHOUSE_CLIENT} --query="SELECT throwIf(number >= 1, 'forced error'), number FROM numbers(10) INTO OUTFILE '${OUT_FILE_8}' TRUNCATE FORMAT TSV SETTINGS max_block_size=1" 2>/dev/null
actual=$(cat "${OUT_FILE_8}" 2>/dev/null)
if [ "$actual" != "old_content" ]; then
    echo "Content mismatch in test_atomic_8: original file not preserved after error"
    echo "Expected: old_content"
    echo "Got: $actual"
    exit 1
fi
rm -f "${OUT_FILE_8}"

echo "OK"
