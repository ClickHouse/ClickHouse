#!/usr/bin/env bash
# Tags: no-parallel, zookeeper
# no-parallel: the failpoint is server-wide and would park the refresh start of every coordinated view on the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="rdb_$CLICKHOUSE_DATABASE"

# Drops the database, and disables the failpoint on an early exit (the disables below resume it otherwise).
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT refresh_mv_pause_inside_coordination_write" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db" 2>/dev/null || true
' EXIT

# 1. A `SYSTEM STOP VIEW` made while the Keeper write that starts a refresh is in flight (`mutex` released) cancels
# that refresh; it used to be discarded, and the refresh ran to completion.

$CLICKHOUSE_CLIENT -q "create database $db engine=Replicated('/test/$CLICKHOUSE_DATABASE/rdb', 's1', 'r1')"

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    create materialized view $db.k refresh every 1 year (x Int64)
        engine ReplicatedMergeTree order by x empty as select 1 as x;"

$CLICKHOUSE_CLIENT -q "
    system enable failpoint refresh_mv_pause_inside_coordination_write;
    system refresh view $db.k;"

if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT refresh_mv_pause_inside_coordination_write PAUSE"
then
    echo "FAIL: the refresh did not reach the coordination-write failpoint"
    exit 1
fi

# The mutex is free while the Keeper write is parked, so this stop lands inside the window.
$CLICKHOUSE_CLIENT -q "
    system stop view $db.k;
    system disable failpoint refresh_mv_pause_inside_coordination_write;"

while [ "`$CLICKHOUSE_CLIENT -q "select status from system.view_refreshes where database = '$db' and view = 'k' -- $LINENO" | xargs`" != 'Disabled' ]
do
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "
    select '<1: stop during the coordination write is honored>',
        (select position(exception, 'cancelled') > 0 from system.view_refreshes where database = '$db' and view = 'k'),
        (select count() from $db.k);"

# 2. A `SYSTEM REFRESH VIEW` counted while the start write of an earlier one consumes from the same znode is kept.
# `APPEND` with `insert_deduplicate = 0`, so that rows count refreshes.

$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
    create materialized view $db.k2 refresh every 1 year append (x Int64)
        engine ReplicatedMergeTree order by x empty as select 1 as x settings insert_deduplicate = 0;
    system stop view $db.k2;"

$CLICKHOUSE_CLIENT -q "
    system enable failpoint refresh_mv_pause_inside_coordination_write;
    system refresh view $db.k2;"

if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT refresh_mv_pause_inside_coordination_write PAUSE"
then
    echo "FAIL: the refresh did not reach the coordination-write failpoint"
    exit 1
fi

$CLICKHOUSE_CLIENT -q "
    system refresh view $db.k2;
    system disable failpoint refresh_mv_pause_inside_coordination_write;
    system wait view $db.k2;
    select '<2: a request counted during the start write is kept>', count() from $db.k2;"
