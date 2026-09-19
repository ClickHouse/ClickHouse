#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--input_format_parallel_parsing=1 --max_parsing_threads=2 --min_chunk_bytes_for_parallel_parsing=1048576 --input_format_json_max_object_size=1048576 --max_memory_usage=0"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_segmentation (a String) ENGINE = Memory"

echo "--- garbage without an opening bracket is rejected ---"
python3 -c "import sys; sys.stdout.buffer.write(b'x' * (20 * 1024 * 1024))" 2>/dev/null \
    | ${CLICKHOUSE_CLIENT} $SETTINGS -q "INSERT INTO t_json_segmentation FORMAT JSONEachRow" 2>&1 \
    | grep -c "input_format_json_max_object_size"

echo "--- JSONEachRow data fed to JSONCompactEachRow is rejected ---"
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_CLIENT} $SETTINGS -q "INSERT INTO t_json_segmentation FORMAT JSONCompactEachRow" 2>&1 \
    | grep -c "input_format_json_max_object_size"

echo "--- many small objects still parse ---"
python3 -c "
import sys
for _ in range(5000):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * 1000 + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_CLIENT} $SETTINGS -q "INSERT INTO t_json_segmentation FORMAT JSONEachRow"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(length(a)) FROM t_json_segmentation"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_segmentation"
