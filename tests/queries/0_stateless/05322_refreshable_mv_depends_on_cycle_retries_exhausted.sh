#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A cycle of refreshable views connected by DEPENDS ON must keep refreshing after a refresh fails and
# `refresh_retries` are used up, once the failure goes away. The view whose retries ran out must not
# consume the dependency refreshes it failed to process: its dependencies refresh again only after
# it succeeds, so the cycle would wait for itself forever.
# https://github.com/ClickHouse/ClickHouse/issues/123775

# Waits until `query` returns 1, for up to 60 seconds.
wait_for()
{
    local query=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT -q "$query")" == 1 ] && return
        sleep 0.1
    done
}

# Prints whether `table` gets new rows within 60 seconds after the failure is gone.
check_resumes()
{
    local table=$1
    local rows
    rows=$($CLICKHOUSE_CLIENT -q "select count() from $table")
    wait_for "select count() > $rows from $table"
    echo "resumed: $($CLICKHOUSE_CLIENT -q "select count() > $rows from $table")"
}

# Failed refresh attempts of `view`, from the query log.
failed_attempts()
{
    local view=$1
    $CLICKHOUSE_CLIENT -q "
        system flush logs query_log;
        select count() from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_kind = 'Insert' and type = 'ExceptionWhileProcessing'
          and log_comment like 'refresh of %.$view%'"
}

echo '-- the REFRESH EVERY view of the cycle fails'
# Each refresh of `head` fails while `fail_head` has a row. The aggregate keeps the throw at execution time.
$CLICKHOUSE_CLIENT -q "
    create table fail_head (x UInt8) engine MergeTree order by x;
    create materialized view head refresh every 1 second depends on tail settings refresh_retries = 0
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail_head;
    create materialized view tail refresh depends on head
        append (x Int64) engine MergeTree order by x
        as select 1 as x;
    system refresh view head;"

wait_for "select count() >= 2 from tail"
$CLICKHOUSE_CLIENT -q "insert into fail_head values (1)"
wait_for "
    select count() = 1 from system.view_refreshes
    where database = currentDatabase() and view = 'head' and exception != ''"
$CLICKHOUSE_CLIENT -q "truncate table fail_head"
check_resumes tail

echo '-- a DEPENDS ON view of the cycle fails'
$CLICKHOUSE_CLIENT -q "
    create table fail_member (x UInt8) engine MergeTree order by x;
    create materialized view head2 refresh every 1 second depends on member
        append (x Int64) engine MergeTree order by x
        as select 1 as x;
    create materialized view member refresh depends on head2
        settings refresh_retries = 0, refresh_retry_max_backoff_ms = 500
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail_member;
    system refresh view head2;"

wait_for "select count() >= 2 from member"
$CLICKHOUSE_CLIENT -q "insert into fail_member values (1)"
for _ in {1..120}
do
    [ "$(failed_attempts member)" -ge 3 ] && break
    sleep 0.5
done

# While it keeps failing, `member` retries the same dependency refresh, but not more often than
# `refresh_retry_max_backoff_ms`. Without the limit the attempts come milliseconds apart.
$CLICKHOUSE_CLIENT -q "
    select 'retries are rate limited:', min(gap) >= 0.4 from
    (
        select dateDiff('millisecond', lagInFrame(event_time_microseconds) over (order by event_time_microseconds), event_time_microseconds) / 1000 as gap,
               row_number() over (order by event_time_microseconds) as n
        from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_kind = 'Insert' and type = 'ExceptionWhileProcessing' and log_comment like 'refresh of %.member%'
    )
    where n > 1"

$CLICKHOUSE_CLIENT -q "truncate table fail_member"
check_resumes head2

echo '-- a view of the cycle with an independent dependency fails'
# `member3` also depends on `ext`, which refreshes every second on its own. A new refresh of `ext` is not one
# that the failed attempt tried, so `member3` must refresh right after it, not wait for the backoff (60 s).
$CLICKHOUSE_CLIENT -q "
    create table fail_member3 (x UInt8) engine MergeTree order by x;
    create materialized view ext refresh every 1 second (x Int64) engine MergeTree order by x as select 1 as x;
    create materialized view head3 refresh every 1 second depends on member3
        append (x Int64) engine MergeTree order by x
        as select 1 as x;
    create materialized view member3 refresh depends on head3, ext
        settings refresh_retries = 0, refresh_retry_max_backoff_ms = 60000
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail_member3;
    system refresh view head3;"

wait_for "select count() >= 2 from member3"
$CLICKHOUSE_CLIENT -q "insert into fail_member3 values (1)"
wait_for "
    select count() = 1 from system.view_refreshes
    where database = currentDatabase() and view = 'member3' and exception != ''"
$CLICKHOUSE_CLIENT -q "truncate table fail_member3"
rows=$($CLICKHOUSE_CLIENT -q "select count() from member3")
start=$(date +%s)
wait_for "select count() > $rows from member3"
echo "refreshed before the backoff ran out: $(( $(date +%s) - start < 40 ))"

$CLICKHOUSE_CLIENT -q "
    drop table head;
    drop table tail;
    drop table head2;
    drop table member;
    drop table head3;
    drop table member3;
    drop table ext;
    drop table fail_head;
    drop table fail_member;
    drop table fail_member3;"
