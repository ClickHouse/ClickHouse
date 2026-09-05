#!/usr/bin/env bash
# Tags: shard

# A serialized plan fragment is optimized again on the follower, where `applyStreamDisjointness` could
# let a final `DISTINCT` over partition-disjoint streams skip the merge into one stream. The deserialized
# step no longer knows whether the initiator kept that stream single for a downstream `LIMIT`, `OFFSET`
# or `LIMIT BY`, so it must not parallelize.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE pd_partitions (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k % 8;
    INSERT INTO pd_partitions SELECT number % 4000 FROM numbers(400000);"

# `optimize_distinct_in_order` is pinned off: with a sorted input the final `DISTINCT` merges the streams
# even when it is free to parallelize, which is the shape the guard produces and would make the control
# query below indistinguishable from a guarded one.
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

# The follower merges its streams before deduplicating - the `Resize` belongs to the final `DISTINCT` -
# and does not scatter either. The preliminary `DISTINCT` never resizes, so the count is one or zero.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name = 'Resize' AND plan_step_name = 'Distinct'),
        countIf(name LIKE 'ScatterByPartition%')
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id' AND query_id != initial_query_id"

# Without plan serialization the same partition-disjoint read is deduplicated in parallel, so the check
# above is not vacuous. Which parallel shape it takes is a cost decision that depends on the profile: the
# streams can stay apart (no `Resize` at all), or they can be scattered by the hash of the DISTINCT
# columns (one `Resize` per partition plus one to collect them). Both differ from the guarded shape - one
# `Resize` merging everything and no scatter - so assert only that the local query is not of that shape.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        countIf(name = 'Resize' AND plan_step_name = 'Distinct') = 1
        AND countIf(name LIKE 'ScatterByPartition%') = 0
    FROM system.processors_profile_log
    WHERE initial_query_id = '$query_id_local'"
