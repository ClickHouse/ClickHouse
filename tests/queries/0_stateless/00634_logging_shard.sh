#!/usr/bin/env bash
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


# Check that logs from remote servers are passed from client

# SELECT
true > "$server_logs_file"
$CLICKHOUSE_CLIENT $settings -q "SELECT 1 FROM system.one FORMAT Null"
lines_one_server=$(cat "$server_logs_file" | wc -l)

true > "$server_logs_file"
$CLICKHOUSE_CLIENT $settings -q "SELECT 1 FROM remote('127.0.0.2,127.0.0.3', system, one) FORMAT Null"
lines_two_servers=$(cat "$server_logs_file" | wc -l)

(( $lines_two_servers >= 2 * $lines_one_server )) || echo "Fail: $lines_two_servers $lines_one_server"

# INSERT
$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS null_00634_1"
$CLICKHOUSE_CLIENT $settings -q "CREATE TABLE null_00634_1 (i Int8) ENGINE = Null"

true > "$server_logs_file"
$CLICKHOUSE_CLIENT $settings -q "INSERT INTO null_00634_1 VALUES (0)"
lines_one_server=$(cat "$server_logs_file" | wc -l)

true > "$server_logs_file"
$CLICKHOUSE_CLIENT $settings -q "INSERT INTO TABLE FUNCTION remote('127.0.0.2', '${CLICKHOUSE_DATABASE}', 'null_00634_1') VALUES (0)"
lines_two_servers=$(cat "$server_logs_file" | wc -l)

$CLICKHOUSE_CLIENT $settings -q "DROP TABLE IF EXISTS null_00634_1"
(( $lines_two_servers > $lines_one_server )) || echo "Fail: $lines_two_servers $lines_one_server"


# Clean
rm "$server_logs_file"
