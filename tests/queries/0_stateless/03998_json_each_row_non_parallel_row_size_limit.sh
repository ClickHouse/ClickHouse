#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A single huge JSON object read on the non-parallel path (input_format_parallel_parsing=0)
# must be rejected with a clear "extremely large" error before it can exhaust memory,
# the same way the parallel-parsing segmentation engine already caps per-row size.
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106704

# min_chunk_bytes_for_parallel_parsing=1000000 => per-row cap is 10x = 10000000 bytes.
# The single object below is ~30 MB, well above the cap.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONEachRow --structure="a String" -q "SELECT length(a) FROM table" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# Normal-sized rows on the same path must still parse fine (the cap is per-row, it resets
# at every row boundary), even when the total input is larger than the cap.
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONEachRow --structure="a String" -q "SELECT count(), sum(length(a)) FROM table"

# The same cap must apply during schema inference, i.e. when no structure is given (the issue's
# actual reproducer is file(..., 'JSONEachRow') without a structure). Otherwise the schema reader
# buffers the whole huge object while inferring the first row and OOMs before the read path is reached.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONEachRow -q "DESC table" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# Schema inference of normal-sized rows without a structure must still work.
python3 -c "
import sys
for i in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"y\",\"n\":%d}\n' % i)
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONEachRow -q "SELECT count(), sum(n) FROM table"

# JSONAsString reads the whole object into a single String column through its own readJSONObject,
# bypassing the JSONEachRow row reader, so it needs the cap on its own scan loop too.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONAsString --structure="json String" -q "SELECT length(json) FROM table" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# Normal-sized objects read as JSONAsString must still parse (per-row cap resets each object).
# NOT ignore(json) forces the column to be read so the capped readJSONObject path is exercised.
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONAsString --structure="json String" -q "SELECT count() FROM table WHERE NOT ignore(json)"

# JSONAsObject reads the whole object into a single JSON column through its own readJSONObject,
# so it must be capped the same way. The query materializes json (SELECT json ... FORMAT Null):
# SELECT 1 / count() requests no input columns, so with optimize_count_from_files it can be
# planned via countRows and skip readJSONObject entirely, leaving the cap untested.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONAsObject --structure="json JSON" -q "SELECT json FROM table FORMAT Null" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# Normal-sized objects read as JSONAsObject must still parse.
# NOT ignore(json) forces the column to be read so the capped readJSONObject path is exercised.
python3 -c "
import sys
for i in range(20):
    sys.stdout.buffer.write(b'{\"a\":%d}\n' % i)
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSONAsObject --structure="json JSON" -q "SELECT count() FROM table WHERE NOT ignore(json)"

# The JSON format (with metadata) falls back to the JSONEachRow reader when readPrefix finds no
# metadata (parse_as_json_each_row), so a single huge no-metadata object reaches the same base
# readRow path and must be capped there too. Materialize the column (SELECT a ... FORMAT Null):
# SELECT 1 / count() can be planned via countRows with optimize_count_from_files and skip the row
# reader, leaving the cap untested.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSON --structure="a String" -q "SELECT a FROM table FORMAT Null" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# Schema inference of the JSON format also falls back to JSONEachRowSchemaReader on no metadata
# (fallback_to_json_each_row), so an oversized no-metadata object must be capped during inference too.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"a\":\"' + b'x' * (30 * 1024 * 1024) + b'\"}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSON -q "DESC table" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL"

# A metadata-framed JSON document keeps the cap disabled (its rows are inside a "data" array, not
# standalone objects), so a large value there must still parse.
python3 -c "import sys; sys.stdout.buffer.write(b'{\"meta\":[{\"name\":\"a\",\"type\":\"String\"}],\"data\":[{\"a\":\"' + b'z' * (1024 * 1024) + b'\"}]}\n')" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSON --structure="a String" -q "SELECT length(a) FROM table"

# Normal-sized no-metadata JSON must still parse on the fallback read path.
python3 -c "
import sys
for _ in range(20):
    sys.stdout.buffer.write(b'{\"a\":\"' + b'y' * (1024 * 1024) + b'\"}\n')
" 2>/dev/null \
    | ${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=0 --min_chunk_bytes_for_parallel_parsing=1000000 --max_memory_usage=0 \
        --input-format=JSON --structure="a String" -q "SELECT count(), sum(length(a)) FROM table WHERE NOT ignore(a)"
