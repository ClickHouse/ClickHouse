#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A view refreshed only by DEPENDS ON (no REFRESH EVERY) whose refresh keeps failing must wait for the
# dependency's next refresh once `refresh_retries` are used up, instead of starting again right away, forever.
# https://github.com/ClickHouse/ClickHouse/issues/123360

# Each refresh fails while `fail` has a row. The aggregate keeps the throw at execution time.
$CLICKHOUSE_CLIENT -q "
    create table fail (x UInt8) engine MergeTree order by x;
    insert into fail values (1);
    create materialized view v_zero refresh depends on a settings refresh_retries = 0
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail;
    create materialized view v_after refresh after 1 second depends on a settings refresh_retries = 0
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail;
    create materialized view v_retry refresh depends on a
        settings refresh_retries = 2, refresh_retry_initial_backoff_ms = 1, refresh_retry_max_backoff_ms = 1
        append (x Int64) engine MergeTree order by x
        as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail;
    -- The dependency refreshes once, when created.
    create materialized view a refresh every 1 year (x Int64) engine MergeTree order by x as select 1 as x;"

# Failed refresh attempts of each view, as one line: v_after v_retry v_zero.
attempts()
{
    $CLICKHOUSE_CLIENT -q "
        system flush logs query_log;
        select arrayStringConcat(groupArray(c), ' ') from (
            select v, countIf(log_comment like 'refresh of %.' || v || '%') as c
            from system.query_log
            array join ['v_after', 'v_retry', 'v_zero'] as v
            where event_date >= yesterday() and current_database = currentDatabase()
              and query_kind = 'Insert' and type = 'ExceptionWhileProcessing'
            group by v order by v)"
}

# Waits until each view has made the expected number of failed attempts and is waiting for the dependency.
wait_for_attempts()
{
    local expected=$1
    for _ in {1..300}
    do
        local statuses
        statuses=$($CLICKHOUSE_CLIENT -q "
            select arrayStringConcat(groupArray(status), ' ') from (
                select toString(status) as status from system.view_refreshes
                where database = currentDatabase() and view != 'a' order by view)")
        if [ "$(attempts)" == "$expected" ] && [ "$statuses" == 'WaitingForDependencies WaitingForDependencies WaitingForDependencies' ]
        then
            break
        fi
        sleep 0.2
    done
    # Without the fix the views keep starting new refreshes, so give them a moment to show it.
    sleep 1
    echo "attempts: $(attempts)"
    $CLICKHOUSE_CLIENT -q "
        select view, status, exception != '' from system.view_refreshes
        where database = currentDatabase() and view != 'a' order by view"
}

echo '-- the first refresh of a: one attempt each, plus 2 retries for v_retry'
wait_for_attempts '1 3 1'

echo '-- the second refresh of a: the same again'
$CLICKHOUSE_CLIENT -q "system refresh view a; system wait view a;"
wait_for_attempts '2 6 2'

echo '-- the third refresh of a, now succeeding'
$CLICKHOUSE_CLIENT -q "truncate table fail; system refresh view a; system wait view a;"
for _ in {1..300}
do
    [ "$($CLICKHOUSE_CLIENT -q "
        select count() from system.view_refreshes
        where database = currentDatabase() and view != 'a'
          and last_success_time is not null and status = 'WaitingForDependencies'")" == 3 ] && break
    sleep 0.2
done
echo "attempts: $(attempts)"
$CLICKHOUSE_CLIENT -q "
    select view, status, exception != '', last_success_time is not null from system.view_refreshes
    where database = currentDatabase() and view != 'a' order by view"

$CLICKHOUSE_CLIENT -q "
    drop table v_zero;
    drop table v_after;
    drop table v_retry;
    drop table a;
    drop table fail;"
