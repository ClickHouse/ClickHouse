#!/usr/bin/env bash
# Tags: memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A refresh that always fails, with both retry-backoff settings at INT64_MAX, so
# backoff() returns a delay that used to overflow the retry instant.
# Pinning refresh_retry_initial_backoff_ms too is required: with the default 100
# and refresh_retries = 10 the multiplier stays under 512 and nothing overflows.
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
    retry=$($CLICKHOUSE_CLIENT -q "select retry from system.view_refreshes where view = 'rmv' and database = currentDatabase()" | xargs)
    if ! [ "$retry" -ge 1 ] 2>/dev/null || [ "$retry" -ge 10 ]; then
        progressed=1
        break
    fi
    sleep 0.2
done
echo "retry_within_budget $((1 - progressed))"

# Scheduled (not WaitingForDependencies) proves the saturated instant stayed
# strictly below time_point::max(), which is the "no refresh scheduled" sentinel.
$CLICKHOUSE_CLIENT -q "
    select 'retry_state', status, next_refresh_time is not null
    from system.view_refreshes where view = 'rmv' and database = currentDatabase()"

# Non-regression: a plain near-future schedule still gets an exact, readable
# instant. 04707 does not bound any schedule period.
$CLICKHOUSE_CLIENT -q "
    create materialized view sched refresh every 1 year
        settings all_replicas = 1
        append (x Int64) engine Memory as select 1 as x;"
$CLICKHOUSE_CLIENT -q "
    select 'sched_state', status, next_refresh_time > now()
    from system.view_refreshes where view = 'sched' and database = currentDatabase()"

# Non-regression: a long-but-representable schedule period is not truncated.
# 1e9 seconds is 31.7 years, which keeps the resulting delay nanosecond-safe with
# a 9x margin and stays inside the UInt32 range next_refresh_time is read through.
$CLICKHOUSE_CLIENT -q "
    create materialized view longsched refresh after 1000000000 second
        settings all_replicas = 1
        append (x Int64) engine Memory as select 1 as x;"
$CLICKHOUSE_CLIENT -q "
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
$CLICKHOUSE_CLIENT -q "drop table rmv"
