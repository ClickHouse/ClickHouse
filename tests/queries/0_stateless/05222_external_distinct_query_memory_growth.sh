#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-distinct-query-memory-growth.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Hash-table growth must respect the 90 MiB spill threshold before a replacement allocation can
# exceed the 160 MiB query limit. No user memory limit protects the query. The input crosses the
# resize boundary and then repeats one block of keys to exercise suppression after spilling.
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1 --query "
    SELECT count(), sum(k)
    FROM
    (
        SELECT DISTINCT toUInt64(number % 2162688) AS k FROM numbers(2228224)
    )
    SETTINGS max_threads = 1, max_block_size = 65536,
        max_memory_usage = 167772160, max_memory_usage_for_user = 0,
        max_untracked_memory = 0, max_bytes_ratio_before_external_distinct = 0,
        max_bytes_before_external_distinct = 94371840,
        optimize_distinct_in_order = 0, allow_preliminary_distinct_abandoning = 0,
        log_comment = 'query_memory_growth';
    SYSTEM FLUSH LOGS query_log;
    SELECT count(), countIf(ProfileEvents['ExternalDistinctWritePart'] > 0)
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
        AND log_comment = 'query_memory_growth'"
