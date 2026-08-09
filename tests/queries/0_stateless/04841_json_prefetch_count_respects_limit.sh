#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: uses an object storage disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `filesystem_prefetches_limit` is a maximum number of prefetches. One JSON column expands into
# many substreams, and the Wide reader issues one prefetch (and one live read buffer) per
# substream, so the limit has to bound the substream count, not the column count.

table="${CLICKHOUSE_DATABASE}.t_json_prefetch"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE ${table} (jn Nullable(JSON), ja Array(JSON)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         disk = disk(type = 'local_blob_storage', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_blob/');

-- Many distinct JSON path types expand Object -> Dynamic -> Variant into many substreams.
INSERT INTO ${table} SELECT
    toJSONString(map('a' || toString(number % 40),
        multiIf(number % 4 = 0, toString(number),
                number % 4 = 1, toString(number / 3),
                number % 4 = 2, toString(number % 7 = 0),
                toString(['x', 'y'])))),
    [toJSONString(map('b' || toString(number % 40),
        multiIf(number % 3 = 0, toString(number),
                number % 3 = 1, toString(number / 7),
                toString(['z']))))]
FROM numbers(100);
"

# $1 - filesystem_prefetches_limit, $2 - query, $3 - log_comment
run_and_count_prefetches() {
    ${CLICKHOUSE_CLIENT} --query "$2" --filesystem_prefetches_limit "$1" --log_comment "$3" \
        --remote_filesystem_read_prefetch 1 --remote_filesystem_read_method threadpool \
        --max_threads 1 --filesystem_prefetch_max_memory_usage '1Gi' > /dev/null

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['RemoteFSPrefetches'] <= $1
        FROM system.query_log
        WHERE current_database = '${CLICKHOUSE_DATABASE}' AND log_comment = '$3'
          AND type = 'QueryFinish' AND event_date >= yesterday()
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

echo "-- prefetches within the limit"
run_and_count_prefetches 5 "SELECT count() FROM ${table} WHERE length(JSONAllPaths(jn)) >= 0" "04841_limit_5_${CLICKHOUSE_TEST_UNIQUE_NAME}"
run_and_count_prefetches 10 "SELECT count() FROM ${table} WHERE length(JSONAllPaths(jn)) >= 0" "04841_limit_10_${CLICKHOUSE_TEST_UNIQUE_NAME}"
# Array(JSON) goes through the same per-substream issuance point, so the bound is wrapper-agnostic.
run_and_count_prefetches 5 "SELECT count() FROM ${table} WHERE length(ja) >= 0" "04841_limit_5_array_${CLICKHOUSE_TEST_UNIQUE_NAME}"

echo "-- the limit is observed, not naturally small: zero means unlimited"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${table} WHERE length(JSONAllPaths(jn)) >= 0" \
    --filesystem_prefetches_limit 0 --log_comment "04841_unlimited_${CLICKHOUSE_TEST_UNIQUE_NAME}" \
    --remote_filesystem_read_prefetch 1 --remote_filesystem_read_method threadpool \
    --max_threads 1 --filesystem_prefetch_max_memory_usage '1Gi' > /dev/null
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['RemoteFSPrefetches'] > 10
    FROM system.query_log
    WHERE current_database = '${CLICKHOUSE_DATABASE}'
      AND log_comment = '04841_unlimited_${CLICKHOUSE_TEST_UNIQUE_NAME}'
      AND type = 'QueryFinish' AND event_date >= yesterday()
    ORDER BY event_time_microseconds DESC LIMIT 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
