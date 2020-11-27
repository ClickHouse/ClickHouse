#!/usr/bin/env bash
# shellcheck disable=SC2009

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS user"
$CLICKHOUSE_CLIENT --query "CREATE USER user IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"

# False positive result due to race condition with sleeps is Ok.

$CLICKHOUSE_CLIENT --user user --password hello --query "SELECT sleep(1)" &
bg_query=$!

# Wait for query to start executing. At that time, the password should be cleared.
for _ in {1..20}
do
    if $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" | grep -q 'SELECT sleep(1)'
    then
        break
    fi

    if ! kill -0 -- $bg_query
    then
        >&2 echo "The SELECT sleep(1) query finished earlier that we could grep for it in the process list, but it should have run for at least one second. Looks like a bug"
    fi
done

ps auxw | grep -F -- '--password' | grep -F hello ||:
# Check that it is still running
kill -0 -- $bg_query
wait

# Once again with different syntax
$CLICKHOUSE_CLIENT --user user --password=hello --query "SELECT sleep(1)" &
bg_query=$!

# Wait for query to start executing. At that time, the password should be cleared.
for _ in {1..20}
do
    if $CLICKHOUSE_CLIENT --query "SHOW PROCESSLIST" | grep -q 'SELECT sleep(1)'
    then
        break
    fi

    if ! kill -0 -- $bg_query
    then
        >&2 echo "The SELECT sleep(1) query finished earlier that we could grep for it in the process list, but it should have run for at least one second. Looks like a bug"
    fi
done

ps auxw | grep -F -- '--password' | grep -F hello ||:
# Check that it is still running
kill -0 -- $bg_query
wait

$CLICKHOUSE_CLIENT --query "DROP USER user"
