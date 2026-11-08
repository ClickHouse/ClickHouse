#!/usr/bin/env bash
# Tags: long, zookeeper, no-shared-merge-tree

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop table if exists rmt2 sync;"

$CLICKHOUSE_CLIENT -q "create table rmt1 (a int, b int)
    engine = ReplicatedMergeTree('/test/02255/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', 'r1') order by a settings old_parts_lifetime=100500;"

$CLICKHOUSE_CLIENT -q "create table rmt2 (a int, b int)
    engine = ReplicatedMergeTree('/test/02255/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', 'r2') order by a settings old_parts_lifetime=100500;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (1, 1), (1, 2), (1, 3);"
$CLICKHOUSE_CLIENT -q "alter table rmt1 update b = b*10 where 1 settings mutations_sync=1"
$CLICKHOUSE_CLIENT -q "system sync replica rmt2;"
$CLICKHOUSE_CLIENT -q "select 1, *, _part from rmt2 order by b;"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and table='rmt1' and name='all_0_0_0'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -f "$path/data.bin"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and table='rmt1' and name='all_0_0_0_1'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -f "$path/data.bin"

$CLICKHOUSE_CLIENT -q "detach table rmt1 sync"
$CLICKHOUSE_CLIENT -q "attach table rmt1" 2>/dev/null

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "select 1, *, _part from rmt1 order by b;"

$CLICKHOUSE_CLIENT -q "truncate table rmt1"

$CLICKHOUSE_CLIENT -q "SELECT table, lost_part_count FROM system.replicas WHERE database=currentDatabase() AND lost_part_count!=0";

$CLICKHOUSE_CLIENT -q "drop table if exists projection_broken_parts_1 sync;"
$CLICKHOUSE_CLIENT -q "drop table if exists projection_broken_parts_1 sync;"
