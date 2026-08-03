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

# The sums above agree whichever format was built, so this arm pins down which one it was. A file
# segmentation engine only ever runs inside ParallelParsingInputFormat, so an error raised by one is
# reachable only if that format was built. Here the object exceeds 10 * min_chunk_bytes.
printf '{"a":"%s"}\n' "$(head -c 32768 /dev/zero | tr '\0' 'x')" > ${CLICKHOUSE_TMP}/big_object.json
$CLICKHOUSE_LOCAL -q "select sum(length(a)) from file('${CLICKHOUSE_TMP}/big_object.json', 'JSONEachRow', 'a String') settings input_format_parallel_parsing=1, max_threads=1, max_parsing_threads=2, min_chunk_bytes_for_parallel_parsing=1024, max_memory_usage=1000000000" 2>&1 \
    | grep -q "min_chunk_bytes_for_parallel_parsing" && echo 'PARALLEL' || echo 'NOT PARALLEL'

rm ${CLICKHOUSE_TMP}/128m.csv ${CLICKHOUSE_TMP}/big_object.json
