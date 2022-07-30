#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

file_name="${CLICKHOUSE_TMP}/res_${CLICKHOUSE_DATABASE}.log"
CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

# Run query via expect to make isatty() return true
function run()
{
    command=$1
    expect << EOF
log_user 0
set timeout 3
match_max 100000

spawn bash -c "$command"
expect 1
EOF

    file "$file_name" | grep -o "ASCII text"
    file "$file_name" | grep -o "with escape sequences"
}

run "$CLICKHOUSE_CLIENT -q 'SELECT 1' 2>$file_name"
run "$CLICKHOUSE_CLIENT -q 'SELECT 1' --server_logs_file=$file_name"

# This query may fail due to bug in clickhouse-client.
# run "$CLICKHOUSE_CLIENT -q 'SELECT 1' --server_logs_file=- >$file_name"

rm -f "$file_name"
