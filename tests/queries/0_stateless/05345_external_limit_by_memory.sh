#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-limit-by-memory.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Every process keeps its own query log and has its own user memory tracker, so a user memory limit is
# independent of other queries and of concurrent stateless tests.
function local_query()
{
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 "$@"
}

# The aggregation over a result shares the memory limits of the query, so only the queries with small results
# check that no key repeats. The others check the number of rows and a checksum of their keys.

# Shows whether each finished query of the process that has a `log_comment` spilled and merged its runs.
SPILL_EVENTS="
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['ExternalLimitByWritePart'] > 0, ProfileEvents['ExternalLimitByMerge'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment != ''
    ORDER BY event_time_microseconds"

# Hash-table growth must respect the 90 MiB spill threshold before a replacement allocation can exceed the
# 160 MiB query limit: for these keys the in-memory `LIMIT BY` replaces a 64 MiB table with a 256 MiB one. No
# user memory limit protects the query. The input crosses the resize boundary and then repeats one block of
# keys, which were counted before the spill, so the window `[1, 3)` keeps exactly the repeated rows.
local_query --query "
    SELECT count(), sum(k), uniqExact(k) = count()
    FROM (SELECT toUInt64(number % 2162688) AS k FROM numbers(2228224) LIMIT 2 OFFSET 1 BY k)
    SETTINGS max_threads = 1, max_block_size = 65536,
        max_memory_usage = 167772160, max_memory_usage_for_user = 0,
        max_untracked_memory = 0, max_bytes_ratio_before_external_limit_by = 0,
        max_bytes_before_external_limit_by = 94371840, log_comment = 'query_memory_growth';
    ${SPILL_EVENTS}"

# A fixed table needs no growth allocation. Its small retained state fits without spill workspace, so the
# query stays in memory under a user limit far below that workspace.
local_query --query "
    SELECT count()
    FROM (SELECT toUInt16(number) AS k FROM numbers(1048576) LIMIT 2 OFFSET 1 BY k)
    SETTINGS max_threads = 1, max_block_size = 65536,
        max_memory_usage = 0, max_memory_usage_for_user = 16777216,
        max_untracked_memory = 0, max_bytes_ratio_before_external_limit_by = 0,
        max_bytes_before_external_limit_by = 1073741824, log_comment = 'fixed_table';
    ${SPILL_EVENTS}"

# Sixty-four wide keys fit the initial hash-table capacity but need additional arena buffers: the in-memory
# `LIMIT BY` exceeds the 128 MiB user limit when it allocates a 64 MiB buffer for them. The spill threshold
# matches the user limit, so hashing must account for projected key-storage growth before allocating.
for key_type in String 'FixedString(1048592)'; do
    local_query --query "
        SELECT count(), sum(length(k)), uniqExact(k) = count()
        FROM
        (
            SELECT CAST(concat(toString(number), repeat('xxxxxxxx', 131072)), '${key_type}') AS k
            FROM numbers(64)
            LIMIT 1 BY k
        )
        SETTINGS max_threads = 1, max_block_size = 2, max_untracked_memory = 0,
            max_memory_usage = 0, max_memory_usage_for_user = 134217728,
            max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 134217728,
            allow_suspicious_fixed_string_types = 1, log_comment = 'arena_growth ${key_type}';
        ${SPILL_EVENTS}"
done

# Expanding the header constant for one input block requires 512 MiB, exceeding the query limit. The spilling
# transform must keep it constant while hashing under an unreachable threshold and after a spill at the first
# chunk, where the ordering also exercises the order restoration. The checksum keeps the payload in the plan.
queries=""
for threshold in 1099511627776 1; do
    queries+="
        SELECT count(), sum(cityHash64(k, payload)), groupArray(k) = arrayReverseSort(groupArray(k))
        FROM
        (
            SELECT number % 4096 AS k, repeat('x', 65536) AS payload
            FROM numbers(32768)
            ORDER BY k + 1 DESC
            LIMIT 2 OFFSET 1 BY k
        )
        SETTINGS max_threads = 1, max_block_size = 8192,
            max_memory_usage = 134217728, max_untracked_memory = 0,
            max_bytes_ratio_before_external_limit_by = 0,
            max_bytes_before_external_limit_by = ${threshold},
            log_comment = 'header_constants threshold=${threshold}';
        "
done
local_query --query "${queries} ${SPILL_EVENTS}"

# The spill ratio applies to the user memory limit. With spilling disabled, the table of the array keys
# outgrows the limit. A ratio of 0.5 sets the threshold to half of the available user memory, so the query
# spills its groups as fingerprints and succeeds. The input repeats one block of keys, as above.
ratio_query="
    SELECT count(), sum(k[1]), uniqExact(k) = count()
    FROM (SELECT [toUInt64(number % 1048576)] AS k FROM numbers(1114112) LIMIT 2 OFFSET 1 BY k)
    SETTINGS max_threads = 1, max_block_size = 65536,
        max_memory_usage = 0, max_memory_usage_for_user = 167772160,
        max_untracked_memory = 0, max_bytes_before_external_limit_by = 0"
local_query --query "${ratio_query}, max_bytes_ratio_before_external_limit_by = 0" 2>&1 | grep -oF 'MEMORY_LIMIT_EXCEEDED' | head -1
local_query --query "${ratio_query}, max_bytes_ratio_before_external_limit_by = 0.5, log_comment = 'ratio 0.5'; ${SPILL_EVENTS}"

# The spill files count toward the disk quota of the query. Exceeding it fails the query after its first runs,
# and the query releases its temporary files: none remains open after the failure, and none is left on disk.
local_query --ignore-error --query "
    SELECT count()
    FROM (SELECT intHash64(number) % 1000000 AS k, number AS v FROM numbers(2228224) LIMIT 2 OFFSET 1 BY k)
    SETTINGS max_threads = 1, max_block_size = 65536, max_untracked_memory = 0,
        max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 1,
        max_temporary_data_on_disk_size_for_query = 1048576, log_comment = 'disk_quota';
    SELECT value FROM system.metrics WHERE metric = 'TemporaryFilesForLimitBy';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, errorCodeToName(exception_code), ProfileEvents['ExternalLimitByWritePart'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND log_comment = 'disk_quota'" 2>/dev/null
find "${LOCAL_DIR}/tmp" -type f | wc -l
