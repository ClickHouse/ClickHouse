#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-wrapped-input.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# The constant payload expands before hashing. Filtering nearly all rows would copy it again,
# exceeding the user limit, so the growth check must leave room for the materialized columns.
# A second input block exercises deduplication across the transition to external processing.
for rows in 16384 32768; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
        SELECT count(), sum(k), min(length(s)), max(length(s))
        FROM
        (
            SELECT number % ($rows - 16) AS k, repeat('x', 4096) AS s FROM numbers($rows)
            UNION DISTINCT
            SELECT toUInt64(0) AS k, repeat('x', 4096) AS s
        )
        SETTINGS max_threads = 1, max_block_size = 16384, max_memory_usage = 0,
            max_memory_usage_for_user = 125829120, max_untracked_memory = 0,
            max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1073741824,
            optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"
done
