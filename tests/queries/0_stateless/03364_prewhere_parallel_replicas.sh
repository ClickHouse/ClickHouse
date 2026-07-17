#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


opts=(
  --enable_analyzer=1
  --enable_parallel_replicas=1
  --parallel_replicas_for_non_replicated_merge_tree=1
  --cluster_for_parallel_replicas='parallel_replicas'
  --max_parallel_replicas=3
  --parallel_replicas_min_number_of_granules_to_enable=0
  --parallel_replicas_min_number_of_rows_per_replica=0
)

${CLICKHOUSE_CLIENT} -nq "
  CREATE TABLE t(a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
  INSERT INTO t SELECT number, number FROM numbers_mt(1e7);
"

query_id="${CLICKHOUSE_DATABASE}_prewhere_parallel_replicas_$RANDOM"

${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id="${query_id}" --query="SELECT * FROM t WHERE b < 100000" --format Null

${CLICKHOUSE_CLIENT} -nq "
  SYSTEM FLUSH LOGS query_log;

  -- Check that all data was read during PREWHERE
  SELECT sum(ProfileEvents['RowsReadByPrewhereReaders']) = sum(ProfileEvents['SelectedRows'])
    FROM system.query_log
   WHERE event_date >= yesterday() AND NOT is_initial_query AND initial_query_id = '$query_id' AND type = 'QueryFinish' -- AND current_database = currentDatabase() 
"

