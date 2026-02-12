#!/usr/bin/env bash
# Tags: race

# Test for data race in Context::getAccess() where need_recalculate_access
# was written under a shared lock while being read by another thread.
#
# To trigger the slow path in getAccess(), we use SETTINGS that affect access
# checks: allow_ddl and allow_introspection_functions are among the three
# settings listed in ContextAccessParams::dependsOnSettingName. When such a
# setting is present in the query, applySettingsFromQuery calls setSetting
# which sets need_recalculate_access=true on a context that already has a
# cached access object (populated by Session::makeQueryContextImpl).
# This forces getAccess() to enter the slow recalculation path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_race_access"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_race_access (id UInt64, name String) ENGINE = MergeTree ORDER BY id"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_race_access SELECT number, toString(number) FROM numbers(100)"

TIMEOUT=10

function thread_ddl_setting()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM t_race_access WHERE id IN (SELECT id FROM t_race_access) SETTINGS allow_ddl=1 FORMAT Null" 2>&1 | grep -v -e "^$" || true
    done
}

function thread_introspection_setting()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM t_race_access WHERE id IN (SELECT id FROM t_race_access) SETTINGS allow_introspection_functions=1 FORMAT Null" 2>&1 | grep -v -e "^$" || true
    done
}

for _ in $(seq 1 4); do
    thread_ddl_setting &
done

for _ in $(seq 1 2); do
    thread_introspection_setting &
done

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE t_race_access"

echo "OK"
