#!/usr/bin/env bash
# Tags: no-fasttest

# COVERAGE TEST, not a regression test -- stated plainly because the distinction matters when this
# file is read later. The abort it relates to ("There are no events in epoll", LOGICAL_ERROR from
# Epoll::getManyReady) needs the last replica's receiver deadline to expire inside a single loop
# iteration, and that race could not be produced deterministically from a stateless test: on
# pristine master the stale pending-replacement flag is normally retracted one iteration later by
# checkNewReplica, so the window closes on its own. This test therefore does NOT redden on pristine
# master, and asserting "no LOGICAL_ERROR" would be vacuous (on a release build that error is only a
# failed query, and the pre-fix state is recovered anyway).
#
# What it does assert is the behaviour the fix could plausibly BREAK, which is worth pinning: the
# stop paths now retract a pending replacement unconditionally, so a replacement that the factory
# can still legitimately honour must STILL be waited for and must still deliver its rows. That is a
# positive, specific oracle -- exact row counts -- rather than an absence-of-error one.
#
# use_hedged_requests is randomized by the test runner and SettingsQuirks may lower it, so it is
# pinned on the client command line rather than in a SETTINGS clause.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_hedged_pending;
    CREATE TABLE t_hedged_pending (id UInt64) ENGINE = MergeTree ORDER BY id
        AS SELECT number FROM numbers(100);
"

# receive_data_timeout_ms = 1 makes the change-replica timer expire almost immediately, which is what
# sets next_replica_in_process and queues the offset -- i.e. it puts a replacement in flight on every
# run rather than occasionally. receive_timeout stays high so the receiver deadline does NOT expire:
# this case is about the replacement being honoured, not about the timeout exit.
# interactive_delay is raised because Progress packets otherwise re-arm the receiver deadline.
for hedged in 0 1; do
    echo "-- use_hedged_requests = $hedged"

    # remote() with the '|' replica form, one shard, three replicas: replacements are drawn from the
    # same shard, so the offset genuinely has somewhere to fail over to.
    $CLICKHOUSE_CLIENT --use_hedged_requests "$hedged" \
                       --prefer_localhost_replica 0 \
                       --receive_data_timeout_ms 1 \
                       --receive_timeout 300 \
                       --interactive_delay 100000000 \
                       --fallback_to_stale_replicas_for_distributed_queries 1 \
                       --allow_changing_replica_until_first_data_packet 0 \
                       -q "
        SELECT 'count: ' || count() FROM remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), t_hedged_pending);
        SELECT 'sum: ' || sum(id) FROM remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), t_hedged_pending);
    "

    # The cancelling shape: a LIMIT satisfied long before the scan finishes drives
    # RemoteQueryExecutor::finish -> tryCancel -> sendCancel, which is the stop path that used to
    # leave the pending flag set. The rows must still be correct and the query must still terminate.
    $CLICKHOUSE_CLIENT --use_hedged_requests "$hedged" \
                       --prefer_localhost_replica 0 \
                       --receive_data_timeout_ms 1 \
                       --receive_timeout 300 \
                       --interactive_delay 100000000 \
                       --fallback_to_stale_replicas_for_distributed_queries 1 \
                       --allow_changing_replica_until_first_data_packet 0 \
                       --max_block_size 1 \
                       -q "
        SELECT 'limited: ' || count() FROM (
            SELECT id FROM remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), t_hedged_pending) LIMIT 5
        );
    "
done

# Liveness check, so the row assertions above can never silently degrade into a plain distributed
# query that never puts a replacement in flight. HedgedRequestsChangeReplica is incremented exactly
# where the pending flag is set (the change-replica timeout branch), so a value >= 1 proves the
# state this test is about was actually entered. Without it, a future change to the settings or the
# fixture could make every assertion above pass vacuously.
qid="${CLICKHOUSE_DATABASE}_witness_$RANDOM"
$CLICKHOUSE_CLIENT --use_hedged_requests 1 \
                   --prefer_localhost_replica 0 \
                   --receive_data_timeout_ms 1 \
                   --receive_timeout 300 \
                   --interactive_delay 100000000 \
                   --fallback_to_stale_replicas_for_distributed_queries 1 \
                   --allow_changing_replica_until_first_data_packet 0 \
                   --log_queries 1 --log_profile_events 1 --query_id "$qid" \
                   -q "SELECT count() FROM remote('127.0.0.1|127.0.0.2|127.0.0.3', currentDatabase(), t_hedged_pending) FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
changed=$($CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['HedgedRequestsChangeReplica']
    FROM system.query_log
    WHERE query_id = '$qid' AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY event_time_microseconds DESC LIMIT 1")
[ -n "$changed" ] && [ "$changed" -ge 1 ] && echo "replacement was put in flight" \
    || echo "replacement never started (coverage lost): $changed"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_hedged_pending;"
