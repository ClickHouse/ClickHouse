#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user"
$CLICKHOUSE_CLIENT --query "CREATE USER user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"

# False positive result due to race condition with sleeps is Ok.

$CLICKHOUSE_CLIENT --user user --password hello --query "SELECT sleep(1)" &
sleep 0.1
ps auxw | grep -F -- '--password' | grep -F hello ||:
wait

$CLICKHOUSE_CLIENT --user user --password=hello --query "SELECT sleep(1)" &
sleep 0.1
ps auxw | grep -F -- '--password' | grep -F hello ||:
wait

$CLICKHOUSE_CLIENT --query "DROP USER user"
