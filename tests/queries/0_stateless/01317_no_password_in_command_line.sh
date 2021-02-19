#!/usr/bin/env bash
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user"
$CLICKHOUSE_CLIENT --query "CREATE USER user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"

# False positive result due to race condition with sleeps is Ok.

$CLICKHOUSE_CLIENT --user user --password hello --query "SELECT sleep(1)" &

# Wait for query to start executing. At that time, the password should be cleared.
while true; do
    sleep 0.1
    $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" | grep -q 'SELECT sleep(1)' && break;
done

ps auxw | grep -F -- '--password' | grep -F hello ||:
wait

$CLICKHOUSE_CLIENT --user user --password=hello --query "SELECT sleep(1)" &

while true; do
    sleep 0.1
    $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" | grep -q 'SELECT sleep(1)' && break;
done

ps auxw | grep -F -- '--password' | grep -F hello ||:
wait

$CLICKHOUSE_CLIENT --query "DROP USER user"
