#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY_ID=$RANDOM
$CLICKHOUSE_BENCHMARK --query-id $QUERY_ID --max-concurrency 3 --max-threads 2  -i 1 --query "SELECT 1" 2>&1 | grep -F 'Queries executed' 
$CLICKHOUSE_BENCHMARK --query_id $QUERY_ID --max_concurrency 3 --max_threads 2  -i 1 --query "SELECT 1" 2>&1 | grep -F 'Queries executed' 

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.query_log WHERE current_database = currentDatabase() AND query_id='$QUERY_ID'"