#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: user manipulation is not supported there

# `CREATE TOKEN` is executed as `ALTER USER <current user> ADD IDENTIFIED ...`, so it drops the expired
# authentication methods of the user just like an administrative `ALTER USER` does: rotating short-lived
# tokens never piles dead ones up against `max_authentication_methods_per_user`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05317_${CLICKHOUSE_DATABASE}"

function admin()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

function create_token()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=human_password" -d "$1 FORMAT TSVRaw" | cut -f1
}

# One element per authentication method of the user: `0` for a method without a deadline, `1` for one
# with a deadline (here always an expired one).
function methods()
{
    admin "SELECT arrayMap(x -> toUInt32(x) != 0, valid_until) FROM system.users WHERE name = '${user}'"
}

function cleanup()
{
    admin "DROP USER IF EXISTS ${user}"
}
trap cleanup EXIT

cleanup
admin "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'human_password'"
admin "GRANT CREATE TOKEN ON *.* TO ${user}"

no_ttl="SETTINGS create_token_default_ttl_seconds = 0"

echo "-- A token that is already expired when it is created is kept by the statement that creates it"
create_token "CREATE TOKEN VALID UNTIL '2020-01-01 00:00:00' ${no_ttl}" > /dev/null
methods

echo "-- The next CREATE TOKEN drops it, and keeps the password and the new token"
token=$(create_token "CREATE TOKEN ${no_ttl}")
methods

echo "-- Rotating expired tokens never accumulates them"
for _ in 1 2 3 4 5
do
    create_token "CREATE TOKEN VALID FOR INTERVAL -1 DAY ${no_ttl}" > /dev/null
done
methods

echo "-- The surviving token and the password still authenticate"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=${token}" -d "SELECT 1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=human_password" -d "SELECT 2"
