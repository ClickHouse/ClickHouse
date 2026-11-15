#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_materialized_view_with_bad_select=0 --session_timezone Etc/UTC"`"
db=$CLICKHOUSE_DATABASE
user_1="user_${CLICKHOUSE_TEST_UNIQUE_NAME}_1"
user_2="user_${CLICKHOUSE_TEST_UNIQUE_NAME}_2"

$CLICKHOUSE_CLIENT -q "
drop view if exists mv_test_03327;
drop table if exists test_03327;
drop user if exists $user_1;
drop user if exists $user_2;
"

$CLICKHOUSE_CLIENT -q "
use $db;
create table test_03327 (x Int64) engine MergeTree order by x;
insert into test_03327 select number from numbers(1000);
create materialized view mv_test_03327 refresh every 10 minute append (x Int64) engine Memory empty as select x*10 as x from test_03327;
"

# user with missing system views grant
$CLICKHOUSE_CLIENT -q "
create user if not exists $user_1;
revoke all on *.* from $user_1;
grant select on $db.mv_test_03327 to $user_1;
GRANT SHOW ON $db to $user_1;
"

# this should not work
$CLICKHOUSE_CLIENT -u $user_1 -q "
use $db;
system refresh view mv_test_03327; -- { serverError ACCESS_DENIED }
system start view mv_test_03327; -- { serverError ACCESS_DENIED }
select * from mv_test_03327 format null;
system wait view mv_test_03327; -- { serverError ACCESS_DENIED }
system stop view mv_test_03327; -- { serverError ACCESS_DENIED }
system cancel view mv_test_03327; -- { serverError ACCESS_DENIED }
"

# user with system views grant
$CLICKHOUSE_CLIENT -q "
create user if not exists $user_2;
revoke all on *.* from $user_2;
grant select on $db.mv_test_03327 to $user_2;
grant show on $db to $user_2;
grant system views on $db.* to $user_2;
"

# all these operations should work
$CLICKHOUSE_CLIENT -u $user_2 -q "
use $db;
system refresh view mv_test_03327;
system start view mv_test_03327;
select * from mv_test_03327 format null;
system wait view mv_test_03327;
system stop view mv_test_03327;
system cancel view mv_test_03327;
"

$CLICKHOUSE_CLIENT -q "
drop view mv_test_03327;
drop table test_03327;
drop user $user_1;
drop user $user_2;
"
