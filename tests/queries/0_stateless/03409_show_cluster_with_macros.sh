#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "Output of: SHOW CLUSTER '{default_cluster_macro}'"

$CLICKHOUSE_CLIENT -q "show cluster '{default_cluster_macro}'" | cut -f1-4,6

echo "to match Output of: SHOW CLUSTER 'test_shard_localhost'"

$CLICKHOUSE_CLIENT -q "show cluster 'test_shard_localhost'" | cut -f1-4,6
