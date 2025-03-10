#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "distributed_ddl_entry_format_version=OPENTELEMETRY_ENABLED_VERSION (older then PRESERVE_INITIAL_QUERY_ID_VERSION)"
OPENTELEMETRY_ENABLED_VERSION=4
query_id="$(random_str 10)"
$CLICKHOUSE_CLIENT --distributed_ddl_entry_format_version=$OPENTELEMETRY_ENABLED_VERSION --query_id "$query_id" --distributed_ddl_output_mode=none -q "DROP TABLE IF EXISTS foo ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "SELECT query FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart'"

echo "distributed_ddl_entry_format_version=PRESERVE_INITIAL_QUERY_ID_VERSION"
PRESERVE_INITIAL_QUERY_ID_VERSION=5
query_id="$(random_str 10)"
# Check that serialization will not be broken with new lines in initial_query_id
query_id+=$'\nfoo'
$CLICKHOUSE_CLIENT --distributed_ddl_entry_format_version=$PRESERVE_INITIAL_QUERY_ID_VERSION --query_id "$query_id" --distributed_ddl_output_mode=none -q "DROP TABLE IF EXISTS foo ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
# - normalizeQuery() is required to strip out DDL comment
# - replace() is required to avoid non deterministic behaviour of
#   normalizeQuery() that replaces the identifier with "?" only if it has more
#   then two numbers.
#
# NOTE: no current_database = '$CLICKHOUSE_DATABASE' filter on purpose (since ON CLUSTER queries does not have current_database passed)
$CLICKHOUSE_CLIENT -q "SELECT normalizeQuery(replace(query, currentDatabase(), 'default')) FROM system.query_log WHERE initial_query_id = '$query_id' AND type != 'QueryStart' ORDER BY event_time_microseconds"
