#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-growth.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The table fits below the spill threshold, but replacing its buffer would exceed the user limit.
# Preliminary hashing can release its optional set, and final hashing spills before that allocation.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
    SELECT count()
    FROM (SELECT DISTINCT toUInt64(number) AS k FROM numbers(8388608))
    SETTINGS max_threads = 1, max_block_size = 65536,
        max_memory_usage = 0, max_memory_usage_for_user = 167772160,
        max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
        max_bytes_before_external_distinct = 94371840,
        optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"

# When the first chunk leaves insufficient spill headroom, the empty set is released and that
# chunk becomes an ordinary spill run. All of its rows must reach the output.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
    SELECT count()
    FROM (SELECT DISTINCT toUInt64(number) AS k FROM numbers(262144))
    SETTINGS max_threads = 1, max_block_size = 262144,
        max_memory_usage = 0, max_memory_usage_for_user = 33554432,
        max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
        max_bytes_before_external_distinct = 1073741824,
        optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"

# Fixed tables need no growth allocation. Their small retained state fits without spill workspace.
for key_type in UInt8 UInt16; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
        SELECT count()
        FROM (SELECT DISTINCT to${key_type}(number) AS k FROM numbers(1048576))
        SETTINGS max_threads = 1, max_block_size = 65536,
            max_memory_usage = 0, max_memory_usage_for_user = 16777216,
            max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
            max_bytes_before_external_distinct = 1073741824,
            optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"
done
