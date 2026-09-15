#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-query-memory-growth.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# A 64 MiB table is below the 90 MiB spill threshold, but allocating its 128 MiB replacement would
# exceed the 160 MiB query limit. Both single-stream and preliminary hashing must yield before growth.
# The threshold must apply with no user limit and with a user limit that leaves ample headroom.
for threads in 1 4; do
    for user_limit in 0 1073741824; do
        ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --query "
            SELECT count(), sum(k)
            FROM
            (
                SELECT DISTINCT toUInt64(number % 8388608) AS k FROM numbers_mt(16777216)
            )
            SETTINGS max_threads = $threads, max_block_size = 65536,
                max_memory_usage = 167772160, max_memory_usage_for_user = $user_limit,
                max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
                max_bytes_before_external_distinct = 94371840,
                optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0"
    done
done

# Wide string keys consume query memory while composite keys retain only fixed-size fingerprints in
# the set. Spilling must respect that query memory, and original aggregate-state fingerprints must
# survive serialization so repeated keys remain suppressed.
cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

for spill_threshold in 0 41943040; do
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/generic-keys" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --query "
        SELECT count(), uniqExactArrayMerge(s), sum(length(wide_key))
        FROM
        (
            SELECT DISTINCT
                initializeAggregation('uniqExactArrayState',
                    arrayMap(x -> cityHash64(x + (number % 3) * 1000000), range(63))) AS s,
                repeat('xxxxxxxx', if(number % 3 = 2, 524288, 0)) AS wide_key
            FROM numbers(8)
        )
        SETTINGS max_threads = 1, max_block_size = 2,
            max_memory_usage = 167772160, max_memory_usage_for_user = 0,
            max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
            max_bytes_before_external_distinct = $spill_threshold,
            optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0,
            log_comment = 'generic_key_threshold';
        SYSTEM FLUSH LOGS query_log;
        SELECT count(), countIf(ProfileEvents['ExternalDistinctWritePart'] > 0)
        FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = currentDatabase()
            AND log_comment = 'generic_key_threshold'"
done
