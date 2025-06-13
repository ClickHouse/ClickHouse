#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create table rmt1 (n int) engine=ReplicatedMergeTree('/test/02488/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') order by n
  settings min_bytes_for_wide_part=0, allow_remote_fs_zero_copy_replication=1, storage_policy='s3_cache'"
$CLICKHOUSE_CLIENT -q "create table rmt2 (n int) engine=ReplicatedMergeTree('/test/02488/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '2') order by n
  settings min_bytes_for_wide_part=0, allow_remote_fs_zero_copy_replication=1, storage_policy='s3_cache'"

$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 -q "insert into rmt2 values (42)"
$CLICKHOUSE_CLIENT -q "system sync replica rmt1"

path=$($CLICKHOUSE_CLIENT -q "select path from system.parts where database='$CLICKHOUSE_DATABASE' and table='rmt2' and name='all_0_0_0'")
# ensure that path is absolute before removing
$CLICKHOUSE_CLIENT -q "select throwIf(substring('$path', 1, 1) != '/', 'Path is relative: $path')" || exit
rm -f $path/count.txt

$CLICKHOUSE_CLIENT -q "detach table rmt2 sync"
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level='fatal' -q "attach table rmt2"

$CLICKHOUSE_CLIENT -q "select reason, name from system.detached_parts where database='$CLICKHOUSE_DATABASE' and table='rmt2'"

$CLICKHOUSE_CLIENT -q "drop table rmt2 sync"

$CLICKHOUSE_CLIENT -q "select * from rmt1"

$CLICKHOUSE_CLIENT -q "drop table rmt1"
