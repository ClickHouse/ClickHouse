#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-arena-growth.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Sixty-four wide keys fit the initial hash-table capacity but need additional arena buffers.
# The spill threshold matches the user limit, so hashing must account for projected key-storage
# growth before allocating. Each local query has an isolated user memory budget.
for key_type in String 'FixedString(1048592)'; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
        SELECT count(), sum(length(k))
        FROM
        (
            SELECT DISTINCT CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '$key_type') AS k
            FROM numbers(64)
        )
        SETTINGS max_threads = 1, max_block_size = 2, max_untracked_memory = 0,
            max_memory_usage = 0, max_memory_usage_for_user = 167772160,
            max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 167772160,
            optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0,
            allow_suspicious_fixed_string_types = 1"
done
