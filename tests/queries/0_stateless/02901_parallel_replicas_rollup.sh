#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function were_parallel_replicas_used ()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

    # Not using current_database = '$CLICKHOUSE_DATABASE' as nested parallel queries aren't run with it
    $CLICKHOUSE_CLIENT --query "
        SELECT
            initial_query_id,
            'Used parallel replicas: ' || (ProfileEvents['ParallelReplicasUsedCount'] > 0)::bool::String
        FROM system.query_log
    WHERE event_date >= yesterday()
      AND query_id = '$1' AND type = 'QueryFinish'
    FORMAT TSV"
}

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS nested"
$CLICKHOUSE_CLIENT --query "CREATE TABLE nested (x UInt8) ENGINE = MergeTree ORDER BY () AS Select 1";

query_id="02901_parallel_replicas_rollup-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT \
  --query_id "${query_id}" \
  --max_parallel_replicas 3 \
  --cluster_for_parallel_replicas "test_cluster_one_shard_three_replicas_localhost" \
  --enable_parallel_replicas 1 \
  --parallel_replicas_for_non_replicated_merge_tree 1 \
  --parallel_replicas_min_number_of_rows_per_replica 0 \
  --parallel_replicas_only_with_analyzer 0 \
  --query "
  SELECT 1 FROM nested
  GROUP BY 1 WITH ROLLUP
  ORDER BY max((SELECT 1 WHERE 0));
";
were_parallel_replicas_used $query_id

# It was a bug in analyzer distributed header.
echo "Distributed query with analyzer"
$CLICKHOUSE_CLIENT --query "SELECT 1 FROM remote('127.0.0.{2,3}', currentDatabase(), nested) GROUP BY 1 WITH ROLLUP ORDER BY max((SELECT 1 WHERE 0))"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS nested"


$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS days"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE days
    (
        year Int64,
        month Int64,
        day Int64
    )
    ENGINE = MergeTree()
    ORDER BY year";
$CLICKHOUSE_CLIENT --query "
  INSERT INTO days VALUES (2019, 1, 5), (2019, 1, 15), (2020, 1, 5), (2020, 1, 15), (2020, 10, 5), (2020, 10, 15);
";

# Note that we enforce ordering of the final output because it's not guaranteed by GROUP BY ROLLUP, only the values of count() are
query_id="02901_parallel_replicas_rollup2-$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT \
  --query_id "${query_id}" \
  --max_parallel_replicas 3 \
  --cluster_for_parallel_replicas "test_cluster_one_shard_three_replicas_localhost" \
  --enable_parallel_replicas 1 \
  --parallel_replicas_for_non_replicated_merge_tree 1 \
  --parallel_replicas_min_number_of_rows_per_replica 0 \
  --parallel_replicas_only_with_analyzer 0 \
  --query "SELECT * FROM (SELECT year, month, day, count(*) FROM days GROUP BY year, month, day WITH ROLLUP) ORDER BY 1, 2, 3";

were_parallel_replicas_used $query_id
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS days"
