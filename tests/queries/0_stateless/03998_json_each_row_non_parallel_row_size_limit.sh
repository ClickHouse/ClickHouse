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
