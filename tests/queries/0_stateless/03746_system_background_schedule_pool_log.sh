#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test for MergeTree table (schedule pool)
$CLICKHOUSE_CLIENT -nmq "
  DROP TABLE IF EXISTS test_merge_tree_03745;
  CREATE TABLE test_merge_tree_03745 (x UInt64, y String) ENGINE = MergeTree() ORDER BY x;
  INSERT INTO test_merge_tree_03745 VALUES (1, 'a'), (2, 'b');
  SYSTEM FLUSH LOGS background_schedule_pool_log;
  SELECT DISTINCT database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name, query_id != '' FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'test_merge_tree_03745';
  DROP TABLE test_merge_tree_03745;
"

# Test for Distributed table (distributed pool)
$CLICKHOUSE_CLIENT -nmq "
  DROP TABLE IF EXISTS test_local_03745;
  DROP TABLE IF EXISTS test_distributed_03745;
  CREATE TABLE test_local_03745 (x UInt64) ENGINE = Memory;
  CREATE TABLE test_distributed_03745 (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), test_local_03745);
  -- Pool is created only for async INSERTs
  INSERT INTO test_distributed_03745 SELECT * FROM numbers(10e6) SETTINGS prefer_localhost_replica=0, distributed_foreground_insert=0;
"
# Wait until the distributed table will be flushed via background task
function wait_distributed_background_flush()
{
  for _ in {1..100}; do
    # after distributed flush from background in case of async_insert is enabled it is possible that data will not be flushed to the local table
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${CLICKHOUSE_DATABASE}.test_local_03745"
    if [ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM test_local_03745") -eq "10000000" ]; then
      return
    fi
    sleep 0.1
  done

  return 1
}
if ! wait_distributed_background_flush; then
  echo "test_local_03745 does not contains all data" >&2
  exit 1
fi
$CLICKHOUSE_CLIENT -nmq "
  SYSTEM FLUSH LOGS background_schedule_pool_log;
  SELECT database, table, table_uuid != toUUIDOrDefault(0) AS has_uuid, log_name, max(duration_ms) > 0, query_id != '' FROM system.background_schedule_pool_log WHERE database = currentDatabase() AND table = 'test_distributed_03745' GROUP BY ALL;
  DROP TABLE test_distributed_03745;
  DROP TABLE test_local_03745;
"
