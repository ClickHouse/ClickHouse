#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

yes http://foobarfoobarfoobarfoobarfoobarfoobarfoobar.com | head -c128M > ${CLICKHOUSE_TMP}/128m.csv

# Aggregate over values, not count(): a trivial count sets need_only_count, which disables parallel
# parsing before the memory guard under test is reached.
for max_memory_usage in 52428800 1000000000; do
    $CLICKHOUSE_LOCAL --stacktrace -q "select sum(length(URL)) from file('${CLICKHOUSE_TMP}/128m.csv', 'TSV', 'URL String') settings input_format_parallel_parsing=1, max_threads=1, max_parsing_threads=16, min_chunk_bytes_for_parallel_parsing=10485760, max_memory_usage=$max_memory_usage"
done

# The sums above agree whichever format was built, so this arm pins down which one it was: the guard
# permits parallel parsing (4194304 * 2 * 2 <= 20971520) but the cap is below what that path needs, so
# the error names it. The sequential path completes at this cap.
$CLICKHOUSE_LOCAL -q "select sum(length(URL)) from file('${CLICKHOUSE_TMP}/128m.csv', 'TSV', 'URL String') settings input_format_parallel_parsing=1, max_threads=1, max_parsing_threads=2, min_chunk_bytes_for_parallel_parsing=4194304, max_memory_usage=20971520" 2>&1 \
    | grep -q "While executing ParallelParsingBlockInputFormat" && echo 'PARALLEL' || echo 'NOT PARALLEL'

rm ${CLICKHOUSE_TMP}/128m.csv
