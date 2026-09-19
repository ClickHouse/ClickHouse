#!/usr/bin/env bash
# Tags: long, no-fasttest, no-debug, no-asan, no-tsan, no-msan
# The 1 GiB limit on a single JSON value is hardcoded, so the test needs more than 1 GiB of data to check it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--input_format_parallel_parsing=0 --max_memory_usage=0"

echo "--- a string value of exactly the limit is accepted ---"
python3 -c "
import sys
sys.stdout.buffer.write(b'{\"a\":\"')
chunk = b'y' * (1024 * 1024)
for _ in range(1024):
    sys.stdout.buffer.write(chunk)
sys.stdout.buffer.write(b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT sum(length(a)) FROM table"

echo "--- a string value one byte above the limit is rejected ---"
python3 -c "
import sys
sys.stdout.buffer.write(b'{\"a\":\"')
chunk = b'y' * (1024 * 1024)
for _ in range(1024):
    sys.stdout.buffer.write(chunk)
sys.stdout.buffer.write(b'y\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT sum(length(a)) FROM table" 2>&1 \
    | grep -c "JSON string is too large"

echo "--- an object read as a string above the limit is rejected ---"
python3 -c "
import sys
sys.stdout.buffer.write(b'{\"a\":{\"b\":\"')
chunk = b'y' * (1024 * 1024)
for _ in range(1024):
    sys.stdout.buffer.write(chunk)
sys.stdout.buffer.write(b'\"}}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT sum(length(a)) FROM table" 2>&1 \
    | grep -c "JSON string is too large"
