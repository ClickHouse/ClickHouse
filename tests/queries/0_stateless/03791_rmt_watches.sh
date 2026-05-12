#!/usr/bin/env bash
# Tags: long, no-parallel, no-shared-merge-tree, no-object-storage
# - SMT/MinIO cannot handle this amount of parts in reasonable time

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for too many watches registered for the same path in ZK during waiting for log entries (on TRUNCATE in this test)

$CLICKHOUSE_CLIENT -nm -q "
  SET throw_on_max_partitions_per_insert_block = 0;
  CREATE TABLE t0 (c0 String) ENGINE = ReplicatedSummingMergeTree('/tables/{database}/t0', 'r1') ORDER BY (c0) PARTITION BY (c0) PRIMARY KEY (c0);
  -- Override send_logs_level to suppress 'MergeTreeDataWriter: INSERT query from initial_user default ({}) inserted a block that created parts in 1000 partitions. This is being logged rather than throwing an exception as throw_on_max_partitions_per_insert_block=false.'
  INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(1000) SETTINGS send_logs_level='error';
  TRUNCATE t0;
"

# Make sure there are no "Too many watches for path /tables/X/t0/replicas/r1/log_pointer: Y (This is likely an error)" messages
# (Logs from receive/send ZooKeeper client threads are not propagated to query, so it will be checked with system.text_log for the time TRUNCATE was running)
#
# NOTE: in release the limit for the warning in ZooKeeper::receiveEvent() is 10K, and creating 10KK parts is very slow, so we will create only 1K, and rely on lower limit for sanitizers/debug builds, which is 100 right now
read -r event_time_timestamp query_duration_ms < <($CLICKHOUSE_CLIENT -nm -q "
  system flush logs query_log;
  select toUnixTimestamp(event_time), query_duration_ms from system.query_log where current_database = '$CLICKHOUSE_DATABASE' and query_kind = 'Drop' and type = 'QueryFinish';
")
$CLICKHOUSE_CLIENT -nm -q "
  system flush logs text_log;
  with fromUnixTimestamp($event_time_timestamp) as end_, $query_duration_ms/1000 as duration_
  select * from system.text_log where logger_name = 'ZooKeeperClient' and level = 'Warning' and event_time between end_-duration_ and end_ limit 5;
"
