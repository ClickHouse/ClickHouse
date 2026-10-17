#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the global failpoint `prepared_sets_build_ordered_set_inplace_fail`, which
# would abandon set builds in concurrently running tests.

# A part task that waits on a set another part task is building must not inherit an abandoned build:
# the builder can be stopped for a reason local to it while this task keeps running. An abandoned
# build publishes a null set, which the waiter rebuilds. It used to publish an exception instead,
# which `StorageMergeTree::waitForMutation` reports as a failed mutation for `mutations_sync`. A
# build that really failed is still reported to every waiter rather than retried once per task.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail" 2>/dev/null ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_shared_set_error"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_shared_set_error (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192"

# Several parts, so one mutation produces several part tasks sharing one \`PreparedSetsCache\`.
for i in 0 1 2 3 4 5 6 7; do
    $CLICKHOUSE_CLIENT --query "
    INSERT INTO t_shared_set_error SELECT number + ${i} * 10000000, number FROM numbers(10000)"
done

# The failpoint fires once inside `CreatingSetsTransform::generate` and skips `finishInsert`, so one
# part task's speculative build stops without creating the set, publishing a null set in the shared
# cache entry. Every other part task must still complete via its own build, whether it was already
# parked in `SharedSet::get()` or arrives after the entry was dropped.
#
# Which of those two a sibling takes is a background pool scheduling property, so neither is
# asserted: under a busy pool the part tasks can run far enough apart that none is ever parked.
# `sleepEachRow` holds the building task inside the subquery to make parking the common case, and
# `PreparedSetsCache.AbandonedBuildIsRetryableButRealFailureIsNot` pins what a parked task observes
# without depending on scheduling at all. The row count keeps a block's sleep under the three second
# cap `max_execution_speed` enforces, and a statement-level `max_block_size` cannot help here because
# a mutation task builds its context from the background context.
table_uuid=$($CLICKHOUSE_CLIENT --query "
    SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_shared_set_error'")

expected_increments=0
for step in 1 2 3; do
    multiplier=$((2 * step + 1))
    # How many rows this mutation's set matches, so the total does not depend on the set contents.
    matched=$($CLICKHOUSE_CLIENT --query "
        SELECT count() FROM t_shared_set_error
        WHERE id IN (SELECT number * ${multiplier} FROM numbers(200))")
    expected_increments=$((expected_increments + matched))

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail"
    # A failed mutation here is the regression itself, so let any error surface.
    $CLICKHOUSE_CLIENT --query "
    ALTER TABLE t_shared_set_error UPDATE v = v + 1
    WHERE id IN (SELECT number * ${multiplier} FROM numbers(200) WHERE sleepEachRow(0.01) = 0)
    SETTINGS mutations_sync = 2"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail"
done

echo 'all mutations completed'

# No part may be left with a failure reason.
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_shared_set_error' AND latest_fail_reason != ''"

# Every mutation incremented exactly the rows its own set matched, whichever task built the set, so
# no part lost or double applied an update. `id % 10000000` recovers each row's inserted `v`.
$CLICKHOUSE_CLIENT --query "
SELECT sum(v) - sum(id % 10000000) = ${expected_increments} FROM t_shared_set_error"

# The abandoned build was really rebuilt rather than the failpoint silently not firing: each mutation
# builds its set once per key, so a key built at least twice is one whose first build was abandoned.
# The cache has weak lifetime semantics, so later task waves may legitimately add further builds.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT --max_rows_to_read 0 --query "
SELECT max(builds) >= 2 FROM
(
    SELECT count() AS builds FROM system.text_log
    WHERE event_date >= yesterday() AND logger_name = 'CreatingSetsTransform'
      AND message LIKE 'Building set, key: %'
      AND query_id LIKE concat('${table_uuid}', '::all\\_%')
    GROUP BY message
)"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_shared_set_error"
