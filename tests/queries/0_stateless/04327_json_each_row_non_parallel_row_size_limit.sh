#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

COMMON_SETTINGS="--input_format_parallel_parsing=0 --input_format_json_max_object_size=10485760 --max_memory_usage=0"

echo "--- JSONEachRow: normal rows parse fine ---"
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONEachRow --structure="a String" -q "SELECT count(), sum(length(a)) FROM table"

echo "--- JSONEachRow: schema inference rejects oversized object ---"
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONEachRow -q "DESC table" 2>&1 \
    | grep -c "input_format_json_max_object_size"

echo "--- JSONEachRow: schema inference works for normal rows ---"
python3 -c "
import sys
for i in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"y\",\"n\":%d}\n' % i)
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONEachRow -q "SELECT count(), sum(n) FROM table"

echo "--- JSONAsString: oversized object is rejected ---"
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONAsString --structure="json String" -q "SELECT length(json) FROM table" 2>&1 \
    | grep -c "input_format_json_max_object_size"

echo "--- JSONAsString: normal rows still parse ---"
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONAsString --structure="json String" -q "SELECT count() FROM table WHERE NOT ignore(json)"

echo "--- JSONAsObject: oversized object is rejected ---"
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONAsObject --structure="json JSON" -q "SELECT json FROM table FORMAT Null" 2>&1 \
    | grep -c "input_format_json_max_object_size"

echo "--- JSONAsObject: normal rows still parse ---"
python3 -c "
import sys
for i in range(20):
    sys.stdout.buffer.write(b'{\"a\":%d}\n' % i)
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSONAsObject --structure="json JSON" -q "SELECT count() FROM table WHERE NOT ignore(json)"

echo "--- JSON (with metadata): large value still parses ---"
python3 -c "import sys; sys.stdout.buffer.write(b'{\"meta\":[{\"name\":\"a\",\"type\":\"String\"}],\"data\":[{\"a\":\"' + b'z' * (1024 * 1024) + b'\"}]}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSON --structure="a String" -q "SELECT length(a) FROM table"

echo "--- JSON (no-metadata fallback): normal rows still parse ---"
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} $COMMON_SETTINGS \
        --input-format=JSON --structure="a String" -q "SELECT count(), sum(length(a)) FROM table WHERE NOT ignore(a)"
