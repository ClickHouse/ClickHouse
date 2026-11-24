#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_a="user_04517_a_${CLICKHOUSE_DATABASE}"
user_b="user_04517_b_${CLICKHOUSE_DATABASE}"

query_as()
{
    local user="$1"
    local query="$2"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --query="$query"
}

query_err_as()
{
    local user="$1"
    local query="$2"
    $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --query="$query" 2>&1 || true
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_a}, ${user_b}"
}

trap cleanup EXIT
cleanup

echo "login"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user_a}; CREATE USER ${user_b}"
[ "$(query_as "$user_a" "SELECT 1")" = "1" ]
[ "$(query_as "$user_b" "SELECT 1")" = "1" ]
echo "OK"

echo "grant_create_user"
$CLICKHOUSE_CLIENT --query "DROP USER ${user_b}; CREATE USER OR REPLACE ${user_a}"
grep -q "Not enough privileges" <<<"$(query_err_as "$user_a" "CREATE USER ${user_b}")"
$CLICKHOUSE_CLIENT --query "GRANT CREATE USER ON *.* TO ${user_a}"
query_as "$user_a" "CREATE USER ${user_b}"
[ "$(query_as "$user_b" "SELECT 1")" = "1" ]
echo "OK"

echo "dropped_user"
for _ in 1 2; do
    $CLICKHOUSE_CLIENT --query "CREATE USER OR REPLACE ${user_a}"
    [ "$(query_as "$user_a" "SELECT 1")" = "1" ]
    $CLICKHOUSE_CLIENT --query "DROP USER ${user_a}"
    grep -q "no user with such name" <<<"$(query_err_as "$user_a" "SELECT 1")"
done
echo "OK"
