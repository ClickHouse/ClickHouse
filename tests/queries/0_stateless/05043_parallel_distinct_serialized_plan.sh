#!/usr/bin/env bash
# Tags: shard

# An unordered serialized `DISTINCT` can partition its input on the follower even when the initiator
# applies an `OFFSET`. The offset does not require an order that the input never established.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="max_threads = 4, max_block_size = 1000, log_processors_profiles = 1"

query_id="05043_serialized_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
    SELECT DISTINCT number FROM remote('127.0.0.1', numbers_mt(200000)) OFFSET 3 FORMAT Null
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, $SETTINGS"

query_id_local="05043_local_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$query_id_local" --query "
    SELECT DISTINCT number FROM numbers_mt(200000) FORMAT Null SETTINGS $SETTINGS"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"

# Only follower processors count, so the initiator cannot satisfy the parallelism assertion.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name LIKE 'ScatterByPartition%') > 0,
        countIf(name = 'DistinctTransform') > 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id' AND query_id != initial_query_id"

# The local unordered query also deduplicates in parallel.
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(name LIKE 'ScatterByPartition%') > 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id_local'"
