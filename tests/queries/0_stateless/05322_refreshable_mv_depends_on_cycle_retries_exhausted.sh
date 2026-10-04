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

# Waits until `query` returns 1, for up to 30 seconds.
wait_for()
{
    local query=$1
    for _ in {1..300}
    do
        [ "$($CLICKHOUSE_CLIENT -q "$query")" == 1 ] && return
        sleep 0.1
    done
}

# Prints whether `table` gets new rows within 30 seconds after the failure is gone.
check_resumes()
{
    local table=$1
    local rows
    rows=$($CLICKHOUSE_CLIENT -q "select count() from $table")
    wait_for "select count() > $rows from $table"
    echo "resumed: $($CLICKHOUSE_CLIENT -q "select count() > $rows from $table")"
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
    select countIf(view = 'head' and exception != '' and status != 'Running')
         + countIf(view = 'tail' and status = 'WaitingForDependencies') = 2
    from system.view_refreshes where database = currentDatabase()"
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
wait_for "
    select count() = 1 from system.view_refreshes
    where database = currentDatabase() and view = 'member' and exception != '' and status != 'Running'"

# While it keeps failing, `member` retries the same dependency refresh, but not more often than
# `refresh_retry_max_backoff_ms`: at most about 4 attempts in 2 seconds, not hundreds.
attempts()
{
    $CLICKHOUSE_CLIENT -q "
        system flush logs query_log;
        select countIf(log_comment like 'refresh of %.member%') from system.query_log
        where event_date >= yesterday() and current_database = currentDatabase()
          and query_kind = 'Insert' and type = 'ExceptionWhileProcessing'"
}
before=$(attempts)
sleep 2
after=$(attempts)
echo "retries are rate limited: $(( after - before <= 10 ))"

$CLICKHOUSE_CLIENT -q "truncate table fail_member"
check_resumes head2

$CLICKHOUSE_CLIENT -q "
    drop table head;
    drop table tail;
    drop table head2;
    drop table member;
    drop table fail_head;
    drop table fail_member;"
