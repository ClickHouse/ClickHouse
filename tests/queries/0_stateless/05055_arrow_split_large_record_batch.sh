#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-debug, no-asan, no-msan, no-tsan, no-ubsan
# The test needs more than 2 GiB of String data in a single block, so it is heavy on memory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="$CLICKHOUSE_TMP/${CLICKHOUSE_DATABASE}_large_record_batch.arrows"
trap 'rm -f "$FILE"' EXIT

# Arrow IPC addresses the Utf8/Binary buffers with 32-bit offsets, so 21475 * 100000 bytes of String data
# do not fit into a single record batch and have to be split across several of them.
$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT number AS n, repeat('x', 100000) AS s
    FROM numbers(21475)
    FORMAT ArrowStream
" > "$FILE"

$CLICKHOUSE_LOCAL --max_memory_usage 0 --query "
    SELECT count(), sum(n), sum(length(s)), uniqExact(s)
    FROM file('$FILE', ArrowStream)
"
