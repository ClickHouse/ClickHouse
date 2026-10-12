#!/usr/bin/env bash
# Tags: shard

set -e

# Get all server logs
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="trace"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

cur_name=$(basename "${BASH_SOURCE[0]}")
server_logs_file="${CLICKHOUSE_TMP}/${cur_name}_server.logs"

server_logs="--server_logs_file=$server_logs_file"
rm -f "$server_logs_file"

settings="$server_logs --log_queries=1 --log_query_threads=1 --log_profile_events=1 --log_query_settings=1"


# Check that query-start messages from remote servers reach the client.
# Total log volume varies with settings and background activity.
check_remote_logs()
{
    local query_id=$1
    local query_kind=$2
    local expected=$3
    local remote_queries
    remote_queries=$(awk -v query_id="$query_id" -v query_kind="$query_kind" '
        index($0, "executeQuery: (from ") &&
        index($0, "initial_query_id: " query_id ")") &&
        index($0, ") " query_kind " ") { count++ }
        END { print count + 0 }
    ' "$server_logs_file")
    if (( remote_queries < expected )); then
        echo "Expected at least $expected remote $query_kind query-start messages, got $remote_queries"
        cat "$server_logs_file"
        exit 1
    fi
}

# SELECT
true > "$server_logs_file"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_select"
$CLICKHOUSE_CLIENT $settings --query_id "$query_id" -q "SELECT 1 FROM remote('127.0.0.2,127.0.0.3', system, one) FORMAT Null"
check_remote_logs "$query_id" SELECT 2

# INSERT
$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS null_00634_1"
$CLICKHOUSE_CLIENT $settings -q "CREATE TABLE null_00634_1 (i Int8) ENGINE = Null"

true > "$server_logs_file"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_insert"
$CLICKHOUSE_CLIENT $settings --query_id "$query_id" -q "INSERT INTO TABLE FUNCTION remote('127.0.0.2', '${CLICKHOUSE_DATABASE}', 'null_00634_1') VALUES (0)"
# The remote table function forwards logs from its `DESC TABLE` metadata query.
check_remote_logs "$query_id" DESC 1

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS null_00634_1"


# Clean
rm "$server_logs_file"
