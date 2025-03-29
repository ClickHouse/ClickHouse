#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

test_user="user_03258_$CLICKHOUSE_DATABASE"
second_db="${CLICKHOUSE_DATABASE}_03258"
$CLICKHOUSE_CLIENT -q "
    create user $test_user;
    create database $second_db;
    create table a (x String) engine Memory;
    insert into a values ('hi');
    grant create, insert, select on ${second_db}.* to $test_user; -- no drop yet
    grant table engine on Memory to $test_user;
    grant select on a to $test_user;
    grant system views on ${second_db}.* to $test_user;
"

# TODO: After https://github.com/ClickHouse/ClickHouse/pull/71336 is merged, remove
#       "definer CURRENT_USER sql security definer" part from both queries.

# Check that permissions are checked on creation.
$CLICKHOUSE_CLIENT --user $test_user -q "
    create materialized view ${second_db}.v refresh every 2 second (x String) engine Memory definer CURRENT_USER sql security definer as select * from a; -- {serverError ACCESS_DENIED}
"
$CLICKHOUSE_CLIENT -q "
    grant drop on ${second_db}.* to $test_user;
"
$CLICKHOUSE_CLIENT --user $test_user -q "
    create materialized view ${second_db}.v refresh every 1 second (x String) engine Memory definer CURRENT_USER sql security definer as select * from a;
    system wait view ${second_db}.v;
    select * from ${second_db}.v;
"

# Check that permissions are checked on refresh.
$CLICKHOUSE_CLIENT -q "revoke select on a from $test_user"
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT -q "system refresh view ${second_db}.v"
    res=$($CLICKHOUSE_CLIENT -q "system wait view ${second_db}.v" 2>&1)
    if [ "$?" != 0 ]
    then
        echo "$res" | grep -o -m1 ACCESS_DENIED || echo "expected ACCESS_DENIED error, got:" "$res"
        break
    fi
    sleep 1
done

$CLICKHOUSE_CLIENT -q "alter table ${second_db}.v modify query select throwIf(1) as x"

# Get an exception during query execution.
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT -q "system refresh view ${second_db}.v"
    res=$($CLICKHOUSE_CLIENT -q "system wait view ${second_db}.v" 2>&1)
    if [ "$?" == 0 ]
    then
        echo "unexpected success"
        break
    else
        echo "$res" | grep -o -m1 FUNCTION_THROW_IF_VALUE_IS_NON_ZERO && break
    fi
    sleep 1
done

# Check that refreshes and both kinds of errors appear in query log.
# (The following string needs to be present in this comment to silence check-style warning: **current_database = currentDatabase()**.
#  It's ok that we don't have this condition in the query, we're checking the db name in `tables` column instead.)
$CLICKHOUSE_CLIENT -q "
    system flush logs query_log;
    select replaceAll(toString(type), 'Exception', 'Ex**ption'), interface, client_name, exception != '', has(tables, '${second_db}.v') from system.query_log where event_time > now() - interval 30 minute and log_comment like 'refresh of ${second_db}.v%' group by all order by all;
"

$CLICKHOUSE_CLIENT -q "
    drop database $second_db;
    drop user $test_user;
"
