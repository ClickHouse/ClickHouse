#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: SSL required

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user_no_password="user_04515_sasha_${CLICKHOUSE_DATABASE}"
user_with_password="user_04515_masha_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user_no_password}, ${user_with_password}"
}

query_as()
{
    local user="$1"
    local password="$2"
    local query="$3"

    if [ -n "$password" ]; then
        $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --password="$password" --query="$query"
    else
        $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --query="$query"
    fi
}

query_err_as()
{
    local user="$1"
    local password="$2"
    local query="$3"

    if [ -n "$password" ]; then
        $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --password="$password" --query="$query" 2>&1 || true
    else
        $CLICKHOUSE_CLIENT_BINARY --database="${CLICKHOUSE_DATABASE}" --user="$user" --query="$query" 2>&1 || true
    fi
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "
    CREATE USER ${user_no_password};
    CREATE USER ${user_with_password} IDENTIFIED BY 'qwerty';
"

[ "$(query_as "$user_no_password" "" "SELECT currentUser()")" = "${user_no_password}" ]
[ "$(query_as "$user_with_password" "qwerty" "SELECT currentUser()")" = "${user_with_password}" ]
[ "$(query_as "$user_no_password" "something" "SELECT currentUser()")" = "${user_no_password}" ]
[ "$(query_as "$user_no_password" "something2" "SELECT currentUser()")" = "${user_no_password}" ]

grep -q "Authentication failed" <<<"$(query_err_as "vasya_04515" "" "SELECT currentUser()")"
grep -q "Authentication failed" <<<"$(query_err_as "$user_with_password" "123" "SELECT currentUser()")"

echo "authentication"
echo "OK"
