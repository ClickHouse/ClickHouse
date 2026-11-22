#!/usr/bin/env bash
# Tags: long, zookeeper, no-shared-merge-tree
# no-shared-merge-tree: depend on local fs (remove parts)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop table if exists rmt2 sync;"

$CLICKHOUSE_CLIENT -q "create table rmt1 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '1') order by n
  settings cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime=0"
$CLICKHOUSE_CLIENT -q "create table rmt2 (n int) engine=ReplicatedMergeTree('/test/02369/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/{database}', '2') order by n"

$CLICKHOUSE_CLIENT -q "system stop replicated sends rmt2"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt2 values (0);"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (1);"
$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (2);"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1 pull;"

# There's a stupid effect from "zero copy replication":
# MERGE_PARTS all_1_2_1 can be executed by rmt2 even if it was assigned by rmt1
# After that, rmt2 will not be able to execute that merge and will only try to fetch the part from rmt2
# But sends are stopped on rmt2...

(sleep 5 && $CLICKHOUSE_CLIENT -q "system start replicated sends rmt2") &

$CLICKHOUSE_CLIENT --optimize_throw_if_noop=1 -q "optimize table rmt1;"
$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"

$CLICKHOUSE_CLIENT -q "select 1, *, _part from rmt1 order by n;"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and table='rmt1' and name='all_1_2_1'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -rf $path

$CLICKHOUSE_CLIENT -q "select * from rmt1;" 2>&1 | grep LOGICAL_ERROR
$CLICKHOUSE_CLIENT --min_bytes_to_use_direct_io=1 --local_filesystem_read_method=pread_threadpool -q "select * from rmt1;" 2>&1 | grep LOGICAL_ERROR

$CLICKHOUSE_CLIENT -q "select sleep(0.1) from numbers($(($RANDOM % 30))) settings max_block_size=1 format Null"

$CLICKHOUSE_CLIENT -q "detach table rmt1;"
$CLICKHOUSE_CLIENT -q "attach table rmt1;"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt1 values (3);"
$CLICKHOUSE_CLIENT -q "system sync replica rmt1 pull;"
$CLICKHOUSE_CLIENT -q "optimize table rmt1 final;"

$CLICKHOUSE_CLIENT -q "system sync replica rmt1;"
$CLICKHOUSE_CLIENT -q "select 3, *, _part from rmt1 order by n;"

$CLICKHOUSE_CLIENT -q "drop table rmt1 sync;"
$CLICKHOUSE_CLIENT -q "drop table rmt2 sync;"
