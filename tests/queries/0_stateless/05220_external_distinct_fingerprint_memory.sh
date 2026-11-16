#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-fingerprint-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Generic keys fit the same user budget with spilling disabled or enabled without a spill.
# Each local query has an isolated user memory tracker, independent of concurrent stateless tests.
for ratio in 0 0.5; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
        SELECT count()
        FROM
        (
            SELECT DISTINCT [concat(toString(number), repeat('x', 4096))] AS k FROM numbers(16384)
        )
        SETTINGS max_threads = 1, max_block_size = 2048, max_untracked_memory = 0,
            max_memory_usage = 0, max_memory_usage_for_user = 125829120,
            max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = $ratio,
            optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"
done

# A one-byte threshold starts spilling before the first chunk is inserted. The original rows
# become an ordinary fingerprint run without a suppression run.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
    SELECT count() FROM (SELECT DISTINCT [number] AS k FROM numbers(262144))
    SETTINGS max_threads = 1, max_block_size = 262144, max_untracked_memory = 0,
        max_memory_usage = 0, max_memory_usage_for_user = 50331648,
        max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1,
        optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"
