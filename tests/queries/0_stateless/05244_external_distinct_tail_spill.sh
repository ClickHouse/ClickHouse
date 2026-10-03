#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-tail-spill.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

run_case()
{
    local name=$1
    local threshold=$2
    local query=$3
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" --query "
        ${query}
        SETTINGS max_threads = 1, max_block_size = 4096,
            max_memory_usage = 2147483648, max_untracked_memory = 0,
            max_bytes_before_external_distinct = ${threshold}, max_bytes_ratio_before_external_distinct = 0,
            prefer_external_sort_block_bytes = 1048576, optimize_distinct_in_order = 0,
            allow_preliminary_distinct_abandoning = 1;

        SELECT sumIf(value, event = 'ExternalDistinctWritePart') > 0,
               sumIf(value, event = 'ExternalDistinctMerge'),
               sumIf(value, event = 'ExternalDistinctTailSpilledRows') > 0,
               sumIf(value, event = 'ExternalDistinctTailKeptRows') > 0
        FROM system.events;"
}

# The counters show whether files were written, the final merge count, and whether tail rows were
# spilled or retained. A tail that fits beside the file readers stays in memory.
run_case narrow 33554432 \
    'SELECT count() FROM (SELECT DISTINCT cityHash64(number) AS k FROM numbers(65537))'

# Wide rows and a small spill threshold leave insufficient room for the final tail beside file readers.
# The result checks the row count and total value length after readback.
run_case payload 1048576 \
    "SELECT count(), sum(length(payload)) FROM (SELECT DISTINCT cityHash64(number) AS k, repeat(toString(number % 10), 128) AS payload FROM numbers(32769))"

# Order restoration must return the largest keys when merging files with the retained tail.
run_case ordered 33554432 \
    'SELECT count(), min(k), max(k) FROM (SELECT DISTINCT number AS k FROM numbers(65537) ORDER BY k + 1 DESC LIMIT 10000)'

# A one-byte threshold spills every input chunk. EOF then starts the merge with an empty tail.
run_case empty_tail 1 \
    'SELECT count(), sum(k) FROM (SELECT DISTINCT number AS k FROM numbers(8192))'

# Closing registration also starts the order-restoration pipeline when there is no final tail.
run_case empty_ordered_tail 1 \
    'SELECT count(), sum(k), groupArray(k) = arrayReverseSort(groupArray(k)) FROM (SELECT DISTINCT number AS k FROM numbers(8192) ORDER BY k + 1 DESC)'

# A query that stays in hashing mode finishes without creating temporary runs.
run_case hashing 1073741824 \
    'SELECT count(), sum(k) FROM (SELECT DISTINCT number AS k FROM numbers(8192))'
