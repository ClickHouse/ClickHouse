#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_parallel_replicas_unavailable_shards"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_parallel_replicas_unavailable_shards (n UInt64) ENGINE=MergeTree() ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test_parallel_replicas_unavailable_shards SELECT * FROM numbers(10)"

${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="debug" --skip_unavailable_shards 1 --allow_experimental_parallel_reading_from_replicas 1 --max_parallel_replicas 11  --use_hedged_requests 0 --cluster_for_parallel_replicas 'parallel_replicas' --parallel_replicas_for_non_replicated_merge_tree 1 -q "SELECT count() FROM test_parallel_replicas_unavailable_shards WHERE NOT ignore(*)" 2>&1 

${CLICKHOUSE_CLIENT} --query="DROP TABLE test_parallel_replicas_unavailable_shards"
