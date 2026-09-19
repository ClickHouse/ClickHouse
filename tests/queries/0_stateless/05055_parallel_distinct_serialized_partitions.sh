#!/usr/bin/env bash
# Tags: shard

# Partition-disjoint input remains eligible for independent `DISTINCT` on the follower. A downstream
# `OFFSET` over unordered input does not require the partition streams to be merged before deduplication.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE pd_partitions (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 8;
    INSERT INTO pd_partitions SELECT number % 4000 FROM numbers(400000);"

# Disable sorted deduplication so the processor profiles expose whether disjoint streams remain separate.
SETTINGS="max_threads = 8, allow_parallel_distinct = 1, allow_distinct_partitions_independently = 1,
    force_distinct_partitions_independently = 1, enable_parallel_replicas = 0, optimize_distinct_in_order = 0,
    log_processors_profiles = 1"

query_id="05055_serialized_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$query_id" --query "
    SELECT DISTINCT k FROM remote('127.0.0.1', currentDatabase(), pd_partitions) OFFSET 3 FORMAT Null
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, $SETTINGS"

query_id_local="05055_local_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$query_id_local" --query "
    SELECT DISTINCT k FROM pd_partitions FORMAT Null SETTINGS $SETTINGS"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"

# Both follower and local execution can retain disjoint streams or scatter them. Neither should have
# exactly one merge before final `DISTINCT` with no scattering.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name = 'Resize' AND plan_step_name = 'Distinct') = 1
        AND countIf(name LIKE 'ScatterByPartition%') = 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id' AND query_id != initial_query_id"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name = 'Resize' AND plan_step_name = 'Distinct') = 1
        AND countIf(name LIKE 'ScatterByPartition%') = 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id_local'"
