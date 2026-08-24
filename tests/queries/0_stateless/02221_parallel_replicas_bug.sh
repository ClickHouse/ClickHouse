#!/usr/bin/env bash

# Reconnections are ok.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --enable_parallel_replicas=1  --parallel_replicas_for_non_replicated_merge_tree=1 --cluster_for_parallel_replicas="test_cluster_one_shard_three_replicas_localhost" -m < "$CURDIR"/01099_parallel_distributed_insert_select.sql > /dev/null
