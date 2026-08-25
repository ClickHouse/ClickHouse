#!/usr/bin/env bash
# Tags: shard

# `DistinctStep::serialize` carries neither `limit_hint` nor `has_order_sensitive_post_distinct_limit`,
# and the follower optimizes the deserialized fragment again. Without the guard it would hash-scatter a
# final `DISTINCT` whose output order the initiator's `LIMIT`, `OFFSET` or `LIMIT BY` consumes.

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

# The follower must deduplicate in a single stream: no scatter, and it did run a `DistinctTransform`.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name LIKE 'ScatterByPartition%'),
        countIf(name = 'DistinctTransform') > 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id'"

# The same query without plan serialization does scatter, so the check above is not vacuous.
${CLICKHOUSE_CLIENT} --query "
    SELECT countIf(name LIKE 'ScatterByPartition%') > 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id_local'"
