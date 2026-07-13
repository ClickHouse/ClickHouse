#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test for Distributed table (distributed pool)
$CLICKHOUSE_CLIENT -nmq "
  DROP TABLE IF EXISTS test_local_03745;
  DROP TABLE IF EXISTS test_distributed_03745;
  CREATE TABLE test_local_03745 (x UInt64) ENGINE = Memory;
  CREATE TABLE test_distributed_03745 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_local_03745);
  -- Pool is created only for async INSERTs
  INSERT INTO test_distributed_03745 SELECT * FROM numbers(10e6) SETTINGS prefer_localhost_replica=0, distributed_foreground_insert=0;
"
# Wait for all batches of the distributed flush to complete.
# The INSERT of 10M rows is split into multiple blocks, each flushed separately
# by the background schedule pool. We must wait for ALL batches, not just the first one.
function wait_for_data()
{
  for _ in {1..1000}; do
    result=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM test_local_03745")
    if [ "$result" -eq "10000000" ]; then
      return
    fi
    sleep 0.1
  done
  echo "test_local_03745 does not contain all data (got $result)" >&2
  return 1
}
if ! wait_for_data; then
  exit 1
fi

# After all data has been flushed, verify the log entry exists.
function wait_for_background_schedule_pool_log_entry()
{
  for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS background_schedule_pool_log"
    result=$($CLICKHOUSE_CLIENT -q "SELECT database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name, max(duration_ms) > 0, query_id != '' FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'test_distributed_03745' GROUP BY ALL")
    if [ -n "$result" ]; then
      echo "$result"
      return
    fi
    sleep 0.1
  done
  return 1
}
if ! wait_for_background_schedule_pool_log_entry; then
  echo "background_schedule_pool_log entry not found" >&2
  exit 1
fi

$CLICKHOUSE_CLIENT -nmq "
  DROP TABLE test_distributed_03745;
  DROP TABLE test_local_03745;
"
