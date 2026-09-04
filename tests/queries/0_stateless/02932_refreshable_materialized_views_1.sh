#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"`"

# Whole budget for one wait, both SIGKILL graces and the diagnostic dump included: `timeout -k`
# waits its grace in addition to the primary duration, so the graces come out of the budget.
WAIT_TOTAL_S=30
WAIT_DUMP_S=8
WAIT_KILL_S=2
WAIT_POLL_S=$((WAIT_TOTAL_S - WAIT_DUMP_S - WAIT_KILL_S))

wait_failed() {
    echo "Wait failed for: $1"
    echo "Expected: $2"
    echo "Actual: $3"
    echo "Outcome: $4"
    # Bounded as well: reading system.view_refreshes takes the refresh task's mutex.
    timeout -k "$WAIT_KILL_S" "$((WAIT_DUMP_S - WAIT_KILL_S))" $CLICKHOUSE_CLIENT -q \
        "select * from system.view_refreshes where database = currentDatabase() format Vertical" \
        || echo "view_refreshes dump failed or timed out"
    exit 1
}

# Poll $1 until its output satisfies $2 ('==' or '!=' against $3, or 'no-scheduling', which
# skips the transient internal state system.view_refreshes briefly reports between state
# transitions), leaving the last output in $wait_result. Expiry reports and exits non-zero.
wait_for() {
    local query="$1" op="$2" value="$3" rc remaining out
    local poll_end=$((EPOCHSECONDS + WAIT_POLL_S + WAIT_KILL_S))
    wait_result='(no poll returned)'
    while :
    do
        remaining=$((poll_end - EPOCHSECONDS - WAIT_KILL_S))
        if ((remaining <= 0))
        then
            wait_failed "$query" "$op $value" "$wait_result" \
                "budget exhausted, last poll returned normally"
        fi
        # -k because a client ignoring SIGTERM would keep a bare `timeout` waiting forever.
        out=$(timeout -k "$WAIT_KILL_S" "$remaining" $CLICKHOUSE_CLIENT -q "$query")
        # Not through a pipe: rc would then be xargs' status, not the client's.
        rc=$?
        if ((rc == 0))
        then
            wait_result=$out
            case "$op" in
                # xargs trims the string and turns \t and \n into spaces.
                '==') [ "$(echo "$wait_result" | xargs)" == "$value" ] && return ;;
                '!=') [ "$wait_result" != "$value" ] && return ;;
                no-scheduling) grep -qE $'(^|\t)Scheduling(\t|$)' <<< "$wait_result" || return ;;
            esac
        else
            # `clickhouse-client` returns a server exception's code as its exit status, and 124 and
            # 137 are themselves error codes, so rc cannot say whether `timeout` did the killing.
            wait_failed "$query" "$op $value" "$wait_result" \
                "last poll failed with status $rc, timeout was ${remaining}s"
        fi
        sleep 0.5
    done
}

# For SELECTs that read the status column.
query_no_scheduling() {
    wait_for "$1" no-scheduling ''
    echo "$wait_result"
}

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"


# Basic refreshing.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_a
        refresh every 2 second
        engine Memory
        empty
        as select number as x from numbers(2) union all select rand64() as x;
    select '<1: created view>', exception, view from refreshes;
    show create rmv_a;"
# Wait for any refresh.
wait_for "select last_success_time is null from refreshes -- $LINENO" == '0'
start_time="`$CLICKHOUSE_CLIENT -q "select reinterpret(now64(), 'Int64')"`"
# Check table contents.
$CLICKHOUSE_CLIENT -q "select '<2: refreshed>', count(), sum(x=0), sum(x=1) from rmv_a"
# Wait for table contents to change. All three sites emit the same query text, so each needs
# its own line number to be identifiable.
wait_for "select * from rmv_a order by x format Values -- $LINENO" != ''
res1="$wait_result"
wait_for "select * from rmv_a order by x format Values -- $LINENO" != "$res1"
res2="$wait_result"
# Wait for another change.
wait_for "select * from rmv_a order by x format Values -- $LINENO" != "$res2"
# Check that the two changes were at least 1 second apart, in particular that we're not refreshing
# like crazy. This is potentially flaky, but we need at least one test that uses non-mocked timer
# to make sure the clock+timer code works at all. If it turns out flaky, increase refresh period above.
$CLICKHOUSE_CLIENT -q "
    select '<3: time difference at least>', min2(reinterpret(now64(), 'Int64') - $start_time, 1000);"

# Create a source table from which views will read.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int8) engine Memory as select 1;"

# Switch to fake clock, change refresh schedule, change query.
$CLICKHOUSE_CLIENT -q "
    system test view rmv_a set fake time '2050-01-01 00:00:01';
    system wait view rmv_a;
    system refresh view rmv_a;
    system wait view rmv_a;"
query_no_scheduling "select '<4.1: fake clock>', status, last_success_time, next_refresh_time, progress, read_rows, total_rows, written_rows, retry from refreshes"
$CLICKHOUSE_CLIENT -q "
    alter table rmv_a modify refresh every 2 year;
    alter table rmv_a modify query select x*2 as x from src;
    system wait view rmv_a;"
query_no_scheduling "select '<4.5: altered>', status, last_success_time, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "show create rmv_a;"
# Advance time to trigger the refresh.
$CLICKHOUSE_CLIENT -q "
    select '<5: no refresh>', count() from rmv_a;
    system test view rmv_a set fake time '2052-02-03 04:05:06';"
wait_for "select last_success_time, status from refreshes -- $LINENO" == '2052-02-03 04:05:06 Scheduled'
$CLICKHOUSE_CLIENT -q "select '<6: refreshed>', * from rmv_a;"
query_no_scheduling "select '<7: refreshed>', status, last_success_time, next_refresh_time from refreshes"

# Create a dependent view, refresh it once.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_b refresh every 2 year depends on rmv_a (y Int32) engine MergeTree order by y empty as select x*10 as y from rmv_a;
    show create rmv_b;
    system test view rmv_b set fake time '2052-11-11 11:11:11';
    system refresh view rmv_b;
    system wait view rmv_b;
    select '<7.5: created dependent>', last_success_time from refreshes where view = 'rmv_b';"
# Next refresh shouldn't start until the dependency refreshes.
$CLICKHOUSE_CLIENT -q "select '<8: refreshed>', * from rmv_b;"
query_no_scheduling "select '<9: refreshed>', view, status, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "system test view rmv_b set fake time '2054-01-24 23:22:21';"
wait_for "select status from refreshes where view = 'rmv_b' -- $LINENO" == 'WaitingForDependencies'

# Drop the source table, check that refresh fails and doesn't leave a temp table behind.
$CLICKHOUSE_CLIENT -q "
    select '<9.2: dropping>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();
    drop table src;
    system refresh view rmv_a;"
$CLICKHOUSE_CLIENT -q "system wait view rmv_a; -- { serverError REFRESH_FAILED }"
$CLICKHOUSE_CLIENT -q "
    select '<9.4: dropped>', countIf(name like '%tmp%'), countIf(name like '%.inner%') from system.tables where database = currentDatabase();"

# Create the source table again, check that refresh succeeds (in particular that tables are looked
# up by name rather than uuid).
query_no_scheduling "select '<10: creating>', view, status, next_refresh_time from refreshes"
$CLICKHOUSE_CLIENT -q "
    create table src (x Int16) engine Memory as select 2;
    system test view rmv_a set fake time '2054-01-01 00:00:01';"
wait_for "select status from refreshes where view = 'rmv_b' -- $LINENO" == 'Scheduled'
# Both tables should've refreshed.
$CLICKHOUSE_CLIENT -q "
    select '<11: chain-refreshed rmv_a>', * from rmv_a;
    select '<12: chain-refreshed rmv_b>', * from rmv_b;"
query_no_scheduling "select '<13: chain-refreshed>', view, status, last_success_time, last_refresh_time, next_refresh_time, exception == '' from refreshes"

$CLICKHOUSE_CLIENT -q "
    system test view rmv_b set fake time '2061-01-01 00:00:00';
    truncate src;
    insert into src values (3);
    system test view rmv_a set fake time '2060-02-02 02:02:02';"
wait_for "select next_refresh_time from refreshes where view = 'rmv_b' -- $LINENO" == '2062-01-01 00:00:00'
$CLICKHOUSE_CLIENT -q "
    select '<15: chain-refreshed rmv_a>', * from rmv_a;
    select '<16: chain-refreshed rmv_b>', * from rmv_b;"
query_no_scheduling "select '<17: chain-refreshed>', view, status, next_refresh_time from refreshes"

# Get to WaitingForDependencies state and remove the depencency.
$CLICKHOUSE_CLIENT -q "
    system test view rmv_b set fake time '2062-03-03 03:03:03'"
wait_for "select status from refreshes where view = 'rmv_b' -- $LINENO" == 'WaitingForDependencies'
$CLICKHOUSE_CLIENT -q "
    alter table rmv_b modify refresh every 2 year"
wait_for "select status, last_refresh_time from refreshes where view = 'rmv_b' -- $LINENO" == 'Scheduled 2062-03-03 03:03:03'
query_no_scheduling "select '<18: removed dependency>', view, status, last_success_time, last_refresh_time, next_refresh_time from refreshes where view = 'rmv_b'"
$CLICKHOUSE_CLIENT -q "show create rmv_b;"

# Can't use the same time unit multiple times.
$CLICKHOUSE_CLIENT -q "
    create materialized view rmv_c refresh every 1 second 2 second (x Int64) engine Memory empty as select * from src; -- { clientError SYNTAX_ERROR }"

$CLICKHOUSE_CLIENT -q "
    drop table src;
    drop table rmv_a;
    drop table rmv_b;
    drop table refreshes;"
