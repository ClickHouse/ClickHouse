#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id_prefix=${CLICKHOUSE_DATABASE}_test_benchmark
$CLICKHOUSE_BENCHMARK -i 100 -c 8 <<< "SELECT 1" --query_id_prefix $query_id_prefix 2>/dev/null

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT --query "SELECT countIf(query_id = '$query_id_prefix'), countIf(query_id LIKE '$query_id_prefix%') FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish'"
