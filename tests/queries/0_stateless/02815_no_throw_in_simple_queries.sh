#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export CLICKHOUSE_TERMINATE_ON_ANY_EXCEPTION=1

# The environment variable works as expected:
$CLICKHOUSE_LOCAL --query 'this is wrong' || echo "Failed"
# No exceptions are thrown in simple cases:
$CLICKHOUSE_LOCAL --query "SELECT 1" || echo "Failed"
$CLICKHOUSE_LOCAL --query "SHOW TABLES" || echo "Failed"
$CLICKHOUSE_LOCAL --query "SELECT * FROM system.tables WHERE database = currentDatabase() FORMAT Null" || echo "Failed"

# The same for the client app:
$CLICKHOUSE_CLIENT --query "SELECT 1" || echo "Failed"
$CLICKHOUSE_CLIENT --query "SHOW TABLES" || echo "Failed"
$CLICKHOUSE_CLIENT --query "SELECT * FROM system.tables WHERE database = currentDatabase() FORMAT Null" || echo "Failed"

# Multi queries are ok:
$CLICKHOUSE_LOCAL "SELECT 1; SELECT 2;" || echo "Failed"

# It can run in interactive mode:
function run()
{
    command=$1
    expect << EOF

log_user 0
set timeout 60
match_max 100000

spawn bash -c "$command"

expect ":) "

send -- "SELECT 1\r"
expect "1"
expect ":) "

send -- "exit\r"
expect eof

EOF
}

run "$CLICKHOUSE_LOCAL"
