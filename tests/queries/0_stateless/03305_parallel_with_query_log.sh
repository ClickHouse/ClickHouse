#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# We use SET queries to test PARALLEL WITH here because they don't start additional threads.
query="
    SET custom_a = 1
    PARALLEL WITH
    SET custom_b = 2
    PARALLEL WITH
    SET custom_c = 3
"

generate_query_id()
{
    echo "$(random_str 10)"
}

query_id_1=$(generate_query_id)
$CLICKHOUSE_CLIENT -q "$query" --query_id=${query_id_1} --max_threads=1

query_id_2=$(generate_query_id)
$CLICKHOUSE_CLIENT -q "$query" --query_id=${query_id_2} --max_threads=2

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT '1', length(thread_ids) FROM system.query_log WHERE event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '${query_id_1}' AND type = 'QueryFinish';
    SELECT '2', length(thread_ids) > 1 FROM system.query_log WHERE event_date >= yesterday() AND current_database = '$CLICKHOUSE_DATABASE' AND query_id = '${query_id_2}' AND type = 'QueryFinish';
"
