#!/usr/bin/env bash
# Tags: long, no-debug, no-asan, no-tsan, no-msan
# The 1 GiB limit on a single JSON value is hardcoded, so the test needs more than 1 GiB of data to check it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--input_format_parallel_parsing=0 --max_memory_usage=0"

echo "--- string values summing to more than 1 GiB in a single block ---"
python3 -c "
import sys
for _ in range(9):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (120 * 1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT count(), sum(length(a)) FROM table"

echo "--- objects read as strings summing to more than 1 GiB in a single block ---"
python3 -c "
import sys
for _ in range(9):
    sys.stdout.buffer.write(b'{\"a\":{\"b\":\"' + b'y' * (120 * 1024 * 1024) + b'\"}}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT count(), sum(length(a)) FROM table"
