#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, long
# no-shared-merge-tree: depend on local fs

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop table if exists rmt2 sync;"

$CLICKHOUSE_CLIENT -q "create table rmt1 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '1') order by n;"
$CLICKHOUSE_CLIENT -q "create table rmt2 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '2') order by n;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (1);"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (2);"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt2;"
$CLICKHOUSE_CLIENT -q "system stop merges rmt2;"
$CLICKHOUSE_CLIENT -q "optimize table rmt1 final;"

$CLICKHOUSE_CLIENT -q "select 1, *, _part from rmt1 order by n;"
$CLICKHOUSE_CLIENT -q "select 2, *, _part from rmt2 order by n;"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and table='rmt1' and name='all_0_1_1'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -rf $path

$CLICKHOUSE_CLIENT -q "select * from rmt1;" 2>&1 | grep LOGICAL_ERROR
$CLICKHOUSE_CLIENT --min_bytes_to_use_direct_io=1 --local_filesystem_read_method=pread_threadpool -q "select * from rmt1;" 2>&1 | grep LOGICAL_ERROR

$CLICKHOUSE_CLIENT -q "detach table rmt1;"
$CLICKHOUSE_CLIENT -q "attach table rmt1;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (3);"
$CLICKHOUSE_CLIENT -q "system start merges rmt2;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "optimize table rmt1 final;"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt2;"
$CLICKHOUSE_CLIENT -q "select 3, *, _part from rmt1 order by n;"
$CLICKHOUSE_CLIENT -q "select 4, *, _part from rmt2 order by n;"

$CLICKHOUSE_CLIENT -q "drop table rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop table rmt2 sync;"
