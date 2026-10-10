#!/usr/bin/env bash
# Tags: atomic-database, memory-engine

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A view whose DEPENDS ON target does not exist yet parks in MissingDependencies and has no retry
# timer. The dependency is created EMPTY with a yearly schedule so it never refreshes: the only
# thing that can wake the dependent is the dependency appearing.

$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory as select 1;
    create materialized view dependent refresh every 1 year depends on dep (x Int64) engine Memory as select x from src;"

for _ in $(seq 1 120); do
    [ "$($CLICKHOUSE_CLIENT -q "select status from system.view_refreshes where database = currentDatabase() and view = 'dependent'")" = 'MissingDependencies' ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '1. dependency absent:', status, last_success_time is null from system.view_refreshes where database = currentDatabase() and view = 'dependent';"

$CLICKHOUSE_CLIENT -q "
    create materialized view dep refresh every 1 year (x Int64) engine Memory empty as select x from src;"

for _ in $(seq 1 120); do
    [ "$($CLICKHOUSE_CLIENT -q "select last_success_time is not null from system.view_refreshes where database = currentDatabase() and view = 'dependent'")" = '1' ] && break
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "
    select '2. dependent woken and refreshed:', last_success_time is not null from system.view_refreshes where database = currentDatabase() and view = 'dependent';
    select '3. dependency never refreshed:', last_success_time is null from system.view_refreshes where database = currentDatabase() and view = 'dep';
    select '4. dependent rows:', count() from dependent;
    drop table dependent;
    drop table dep;
    drop table src;"
