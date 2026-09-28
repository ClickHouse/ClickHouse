#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the failpoint is server-wide and would stall the announcements of any
# concurrently running parallel-replicas query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An initiator that finishes before a replica has announced must not wait for that announcement: the
# only packet such a replica still owes is the announcement itself, and it is worthless once we are
# cancelling. `parallel_replicas_delay_announcement` holds every follower just short of announcing,
# so the initiator - which satisfies `LIMIT 1` from its local plan - always cancels them mid-planning.
#
# Both remote-socket modes are exercised, because they leave the cancel by different routes and only
# one of them can be interrupted. With `async_socket_for_remote=1` the read is driven by
# `RemoteQueryExecutorReadContext` and the wait is skipped; with `0` the reading thread sits in a
# blocking socket read that cancellation cannot interrupt, so the initiator still waits out the
# replica. The timing assertion therefore covers the asynchronous mode only - the synchronous one is
# here to prove that skipping the drain does not corrupt the connections or take the server down,
# which is what broke twice while this was being written.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE cancel_before_announcement (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
    INSERT INTO cancel_before_announcement SELECT number, number FROM numbers(200000);
"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT parallel_replicas_delay_announcement"
# Without this the local plan can answer `LIMIT 1` before the query has even been sent to the
# followers, leaving nothing to cancel and nothing to wait for - which an unfixed server passes
# just as easily as a fixed one.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read"

SETTINGS="enable_parallel_replicas = 1
        , max_parallel_replicas = 3
        , cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'
        , parallel_replicas_for_non_replicated_merge_tree = 1
        , parallel_replicas_local_plan = 1"

# The failpoint holds a follower for 3s; an initiator that waits for one cannot come in under this.
MAX_INITIATOR_MS=1500

for async_socket in 0 1; do
    for async_sending in 0 1; do
        label="async_socket=$async_socket async_sending=$async_sending"
        # $$ keeps the id unique across repeated runs against one database, so the assertions
        # below cannot pick up `query_log` rows left by an earlier run.
        query_id="${CLICKHOUSE_DATABASE}_cancel_before_announcement_$$_${async_socket}_${async_sending}"

        combo_settings="$SETTINGS
                      , async_socket_for_remote = $async_socket
                      , async_query_sending_for_remote = $async_sending"

        # `LIMIT 1` straight off the table, so the local plan satisfies it and the followers are
        # cancelled while still held by the failpoint. Wrapping it in an aggregate would spread the
        # work back across the replicas and the initiator would wait for them after all. The row's
        # value depends on which local part answers first, so only success is asserted.
        if $CLICKHOUSE_CLIENT --query_id "$query_id" -q "
            SELECT k FROM cancel_before_announcement LIMIT 1 SETTINGS $combo_settings FORMAT Null
        "; then echo "$label query_succeeded"; else echo "$label QUERY_FAILED"; fi

        $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

        if [[ $async_socket == 1 ]]; then
            # The initiator must not have waited out the followers' planning. This is the
            # regression: without the fix it sits here for the whole `parallel_replicas_delay_announcement`
            # delay, and twice over when the query is also sent to the replicas synchronously.
            $CLICKHOUSE_CLIENT -q "
                SELECT '$label did_not_wait ', max(query_duration_ms) < $MAX_INITIATOR_MS
                FROM system.query_log
                WHERE current_database = currentDatabase()
                  AND query_id = '$query_id' AND type = 'QueryFinish' AND is_initial_query
            "
        fi

        # The failpoint is off for this one only so it does not pay the announcement delay again;
        # the connections left behind by the cancel above are unaffected either way.
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT parallel_replicas_delay_announcement"
        echo -n "$label reads_after_cancel "
        $CLICKHOUSE_CLIENT -q "
            SELECT count(), sum(k) FROM cancel_before_announcement SETTINGS $combo_settings
        "
        $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT parallel_replicas_delay_announcement"
    done
done

# `partial_result_on_first_cancel` is inherited by secondary queries, and on the first `Cancel` it
# makes `processCancel` return without setting `stop_query` - "return what you have" instead of
# stopping. A replica that has not announced yet has nothing to return, and going on to announce
# writes into a socket the initiator has already disconnected (`Broken pipe`). Only the asynchronous
# socket is affected: with the synchronous one the initiator is still blocked reading the
# announcement, so the write lands.
partial_query_id="${CLICKHOUSE_DATABASE}_cancel_before_announcement_$$_1_1_partial"
if $CLICKHOUSE_CLIENT --query_id "$partial_query_id" -q "
    SELECT k FROM cancel_before_announcement LIMIT 1
    SETTINGS $SETTINGS
           , async_socket_for_remote = 1
           , async_query_sending_for_remote = 1
           , partial_result_on_first_cancel = 1
    FORMAT Null
"; then echo "partial_result_on_first_cancel query_succeeded"; else echo "partial_result_on_first_cancel QUERY_FAILED"; fi

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT 'partial_result_on_first_cancel did_not_wait ', max(query_duration_ms) < $MAX_INITIATOR_MS
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id = '$partial_query_id' AND type = 'QueryFinish' AND is_initial_query
"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT parallel_replicas_delay_announcement"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read"

# The cancelled followers outlive the initiator - that is the whole point - so wait for them to be
# gone before looking at what they logged. How many of them got as far as running at all depends on
# how quickly the cancel overtook the query being sent, so the assertion is over what they did, not
# over how many there were.
for _ in {1..100}; do
    running=$($CLICKHOUSE_CLIENT -q "
        SELECT count() FROM system.processes
        WHERE initial_query_id LIKE '${CLICKHOUSE_DATABASE}_cancel_before_announcement_$$_1_%'
          AND NOT is_initial_query
    ")
    [[ $running -eq 0 ]] && break
    sleep 0.25
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# A replica cancelled while still planning must never go on to write its announcement into a socket
# the initiator has stopped reading - which shows up as `Broken pipe` on the replica.
$CLICKHOUSE_CLIENT -q "
    SELECT 'no_follower_announced ', countIf(ProfileEvents['MergeTreeAllRangesAnnouncementsSent'] != 0) = 0,
           'no_broken_pipe ', countIf(exception LIKE '%Broken pipe%') = 0
    FROM system.query_log
    WHERE initial_query_id LIKE '${CLICKHOUSE_DATABASE}_cancel_before_announcement_$$_1_%'
      AND NOT is_initial_query AND type != 'QueryStart'
"

$CLICKHOUSE_CLIENT -q "DROP TABLE cancel_before_announcement"
