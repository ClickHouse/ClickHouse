#!/usr/bin/env bash
# Tags: memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SET FAKE TIME parses its literal in the session timezone; refresh scheduling is UTC.
CLICKHOUSE_CLIENT="$(echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g')"
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"

# system.view_refreshes briefly reports the transient internal state 'Scheduling' between state
# transitions of the refresh task, so a status read taken right after a DDL can catch it. Same
# helper as 02932_refreshable_materialized_views_1.sh, bounded so a hang is not reported as a pass.
query_no_scheduling() {
    for _ in {1..100}; do
        out=$($CLICKHOUSE_CLIENT -q "$1")
        if ! grep -qE $'(^|\t)Scheduling(\t|$)' <<< "$out"; then
            echo "$out"
            return
        fi
        sleep 0.2
    done
    echo "still_scheduling"
}

retry_of() {
    $CLICKHOUSE_CLIENT -q "select retry from system.view_refreshes
        where view = '$1' and database = currentDatabase()" | xargs
}

# Wait for the view's first refresh to finish. Views without EMPTY start at last_completed_timeslot
# = epoch, so their first refresh is already overdue at CREATE and runs immediately instead of being
# scheduled; until it lands, status is Scheduling/Running and next_refresh_time is the epoch.
wait_first_refresh() {
    for _ in {1..100}; do
        [ "$($CLICKHOUSE_CLIENT -q "select last_success_time is not null from system.view_refreshes
              where view = '$1' and database = currentDatabase()" | xargs)" = "1" ] && return
        sleep 0.2
    done
    echo "no_first_refresh_$1"
}

# A refresh that always fails, with both retry-backoff settings at INT64_MAX, so
# backoff() returns a delay that used to overflow the retry instant.
# Pinning refresh_retry_initial_backoff_ms too is required: with the default 100
# and refresh_retries = 10 the multiplier stays under 512 and nothing overflows.
# Only backoff()'s IF branch is exercised (retry_idx = 0, initial * 1): the 95-year clamped delay
# stops a second retry from happening, so the ELSE branch that returns refresh_retry_max_backoff_ms
# verbatim is unreachable here. That is why the clamp sits after the if/else rather than in a branch.
# all_replicas = 1 keeps the refresh uncoordinated so this client's replica runs it.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv refresh after 1 year
        settings refresh_retries = 10,
                 refresh_retry_initial_backoff_ms = 9223372036854775807,
                 refresh_retry_max_backoff_ms = 9223372036854775807,
                 all_replicas = 1
        append (x Int64) engine Memory as select throwIf(number = 0) as x from numbers(1);"

$CLICKHOUSE_CLIENT -q "system wait view rmv" 2>&1 | grep -qF REFRESH_FAILED && echo "refresh_failed 1"

# The retry instant must stay far in the future, so the view sits on attempt 1
# instead of racing through the whole retry budget. With the overflow the wrapped
# instant landed in the past, so all 11 attempts burned within ~0.1s.
progressed=0
for _ in {1..40}; do
    retry=$(retry_of rmv)
    if ! [ "$retry" -ge 1 ] 2>/dev/null || [ "$retry" -ge 10 ]; then
        progressed=1
        break
    fi
    sleep 0.2
done
echo "retry_within_budget $((1 - progressed))"

# Scheduled (not WaitingForDependencies) proves the saturated instant stayed strictly below
# time_point::max(), which is the "no refresh scheduled" sentinel. The attempt counter must also
# hold still: a wrapped or past-due instant makes doScheduling start a refresh on every pass.
# next_refresh_time cannot be read by value here - the instant is year 2121, and the column is a
# UInt32 DateTime.
before=$(retry_of rmv)
sleep 2
after=$(retry_of rmv)
query_no_scheduling "
    select 'retry_state', status, $before = $after
    from system.view_refreshes where view = 'rmv' and database = currentDatabase()"

# Liveness under a far-future clock, which no real clock reaches: 2e12 seconds is year ~65340, past
# both the bound on last_attempt_time and what std::chrono::year's short can hold. The server must
# stay up and keep reporting a non-sentinel instant. This arm does not distinguish a clamped
# last_attempt_time from an unclamped one - that difference is a silent calendar wrap, and asserting
# it would mean hard-coding a truncated UInt32 DateTime. Unset the clock to stop the retries.
$CLICKHOUSE_CLIENT -q "
    create materialized view clamped refresh after 1 hour
        settings refresh_retries = 1000000,
                 refresh_retry_initial_backoff_ms = 9223372036854775807,
                 refresh_retry_max_backoff_ms = 9223372036854775807,
                 all_replicas = 1
        append (x Int64) engine Memory as select throwIf(number = 0) as x from numbers(1);"
$CLICKHOUSE_CLIENT -q "system wait view clamped" 2>&1 | grep -qF REFRESH_FAILED && echo "clamped_failed 1"
$CLICKHOUSE_CLIENT -q "system test view clamped set fake time '2000000000000'"
retried=0
for _ in {1..100}; do
    if [ "$(retry_of clamped)" -ge 3 ] 2>/dev/null; then
        retried=1
        break
    fi
    sleep 0.2
done
$CLICKHOUSE_CLIENT -q "system test view clamped unset fake time"
echo "clamped_retried $retried"
query_no_scheduling "
    select 'clamped_state', status, next_refresh_time is not null
    from system.view_refreshes where view = 'clamped' and database = currentDatabase()"

# Non-regression: a plain near-future schedule still gets an exact, readable
# instant. 04707 does not bound any schedule period.
$CLICKHOUSE_CLIENT -q "
    create materialized view sched refresh every 1 year
        settings all_replicas = 1
        append (x Int64) engine Memory as select 1 as x;"
wait_first_refresh sched
query_no_scheduling "
    select 'sched_state', status, next_refresh_time > now()
    from system.view_refreshes where view = 'sched' and database = currentDatabase()"

# Non-regression: a long-but-representable schedule period is not truncated.
# 1e9 seconds is 31.7 years, which keeps the resulting delay nanosecond-safe with
# a 9x margin and stays inside the UInt32 range next_refresh_time is read through.
$CLICKHOUSE_CLIENT -q "
    create materialized view longsched refresh after 1000000000 second
        settings all_replicas = 1
        append (x Int64) engine Memory as select 1 as x;"
wait_first_refresh longsched
query_no_scheduling "
    select 'longsched_state', status, next_refresh_time > now()
    from system.view_refreshes where view = 'longsched' and database = currentDatabase()"

# Non-regression smoke for the random-spread path, which this fix does not touch.
# The oracle is deliberately sign-independent: randomness is drawn uniformly from
# both signs, so a negative draw legitimately yields a past-due instant and any
# status assertion here would be a coin flip on master already.
$CLICKHOUSE_CLIENT -q "
    create materialized view spread refresh after 1 hour randomize for 1 hour
        settings refresh_retries = 10,
                 refresh_retry_initial_backoff_ms = 9223372036854775807,
                 refresh_retry_max_backoff_ms = 9223372036854775807,
                 all_replicas = 1
        append (x Int64) engine Memory as select throwIf(number = 0) as x from numbers(1);"
$CLICKHOUSE_CLIENT -q "
    select 'spread_alive', count() from system.view_refreshes
    where view = 'spread' and database = currentDatabase()"

$CLICKHOUSE_CLIENT -q "drop table spread"
$CLICKHOUSE_CLIENT -q "drop table longsched"
$CLICKHOUSE_CLIENT -q "drop table sched"
$CLICKHOUSE_CLIENT -q "drop table clamped"
$CLICKHOUSE_CLIENT -q "drop table rmv"
