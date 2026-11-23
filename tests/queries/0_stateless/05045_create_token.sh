#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: user manipulation is not supported there
# Tag no-replicated-database: the test relies on grants on the current database, and CREATE USER is executed on all the replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05045_${CLICKHOUSE_DATABASE}"
user2="u2_05045_${CLICKHOUSE_DATABASE}"

# All statements go over HTTP: spawning the clickhouse-client binary for each of them dominates
# the test's runtime under sanitizers (each start costs seconds).
function admin()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

function login()
{
    local password=$1
    shift
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=${password}" -d "$@"
}

function login_expect_error()
{
    local password=$1
    local error=$2
    shift 2
    login "$password" "$@" 2>&1 | grep -m1 -o "$error" | head -n 1
}

# Runs a CREATE TOKEN query as the user and prints the raw result row:
# the secret, a tab, and the resolved deadline.
function create_token()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=human_password" -d "$1 FORMAT TSVRaw"
}

function is_token()
{
    echo "$1" | grep -c -E '^[0-9A-Za-z]{32}$'
}

function cleanup()
{
    admin "DROP USER IF EXISTS ${user}, ${user2}, ${user}_renamed"
}
trap cleanup EXIT

cleanup
admin "CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x"
admin "CREATE TABLE t2 (x UInt64) ENGINE = MergeTree ORDER BY x"
admin "INSERT INTO t1 VALUES (1)"
admin "INSERT INTO t2 VALUES (2)"

admin "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'human_password'"
admin "CREATE USER ${user2} IDENTIFIED WITH plaintext_password BY 'other_password'"
admin "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user}"

echo "-- Without the CREATE TOKEN privilege the query is denied, and the message names the privilege"
login "human_password" "CREATE TOKEN" 2>&1 | grep -m1 -o -F "Not enough privileges. To execute this query, it's necessary to have the grant CREATE TOKEN ON *.*" | head -n 1

admin "GRANT CREATE TOKEN ON *.* TO ${user}"

# The tokens below are created with the default TTL disabled, so that the deadlines shown by
# SHOW CREATE USER stay reproducible; the default TTL is covered on its own further down.
no_ttl="SETTINGS create_token_default_ttl_seconds = 0"

echo "-- The token is a 32-character alphanumeric string, and a new one is generated every time"
token=$(create_token "CREATE TOKEN ${no_ttl}" | cut -f1)
another_token=$(create_token "CREATE TOKEN ${no_ttl}" | cut -f1)
is_token "$token"
is_token "$another_token"
test "$token" != "$another_token" && echo "tokens differ"

echo "-- The token authenticates as the user, with the full rights of the user"
login "$token" "SELECT currentUser()" | sed "s/${user}/user/g"
login "$token" "SELECT x FROM t1"
login "$token" "SELECT x FROM t2"

echo "-- The GRANTS clause limits the rights of the sessions authenticated with the token"
limited_token=$(create_token "CREATE TOKEN GRANTS (SELECT ON ${CLICKHOUSE_DATABASE}.t1) ${no_ttl}" | cut -f1)
login "$limited_token" "SELECT x FROM t1"
login_expect_error "$limited_token" "ACCESS_DENIED" "SELECT x FROM t2"

echo "-- The VALID UNTIL clause is stored with the authentication method and is reported back"
dated=$(create_token "CREATE TOKEN VALID UNTIL '2077-01-01 00:00:00' GRANTS (SELECT ON ${CLICKHOUSE_DATABASE}.t2) ${no_ttl}")
dated_token=$(echo "$dated" | cut -f1)
echo "$dated" | cut -f2
login "$dated_token" "SELECT x FROM t2"

echo "-- The generated secrets are stored hashed and are not shown by SHOW CREATE USER"
admin "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"

echo "-- Without a clause the token lives for create_token_default_ttl_seconds, 30 minutes by default"
default_valid_until=$(create_token "CREATE TOKEN" | cut -f2)
admin "SELECT dateDiff('second', now(), toDateTime64('${default_valid_until}', 0)) BETWEEN 1700 AND 1800"

echo "-- The reported deadline is the one stored on the authentication method"
admin "SELECT countEqual(valid_until, toDateTime64('${default_valid_until}', 0)) FROM system.users WHERE name = '${user}'"

echo "-- The setting is honoured, and an explicit clause wins over it"
short_valid_until=$(create_token "CREATE TOKEN SETTINGS create_token_default_ttl_seconds = 60" | cut -f2)
admin "SELECT dateDiff('second', now(), toDateTime64('${short_valid_until}', 0)) BETWEEN 1 AND 60"
explicit_valid_until=$(create_token "CREATE TOKEN VALID UNTIL '2077-01-01 00:00:00' SETTINGS create_token_default_ttl_seconds = 60" | cut -f2)
echo "$explicit_valid_until"

echo "-- A zero setting, and VALID UNTIL 'infinity', create a token which never expires (reported as 0)"
never_valid_until=$(create_token "CREATE TOKEN ${no_ttl}" | cut -f2)
admin "SELECT toDateTime64('${never_valid_until}', 0) = toDateTime64(0, 0)"
infinite_valid_until=$(create_token "CREATE TOKEN VALID UNTIL 'infinity' SETTINGS create_token_default_ttl_seconds = 60" | cut -f2)
admin "SELECT toDateTime64('${infinite_valid_until}', 0) = toDateTime64(0, 0)"

echo "-- An already expired token does not authenticate"
expired_token=$(create_token "CREATE TOKEN VALID FOR INTERVAL -1 DAY ${no_ttl}" | cut -f1)
login_expect_error "$expired_token" "AUTHENTICATION_FAILED" "SELECT 1"

echo "-- A session authenticated with an unlimited token can create tokens"
is_token "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=${token}" -d "CREATE TOKEN FORMAT TSVRaw" | cut -f1)"

echo "-- A session limited by the GRANTS clause cannot, even when CREATE TOKEN is listed and granted"
minting_token=$(create_token "CREATE TOKEN GRANTS (CREATE TOKEN ON *.*) ${no_ttl}" | cut -f1)
login_expect_error "$minting_token" "ACCESS_DENIED" "CREATE TOKEN"
login_expect_error "$minting_token" "ACCESS_DENIED" "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'minted'"

echo "-- The privilege also authorizes the equivalent ALTER USER ADD IDENTIFIED for the current user"
login "human_password" "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'self_added' GRANTS (SELECT ON ${CLICKHOUSE_DATABASE}.t1)"
login "self_added" "SELECT x FROM t1"

echo "-- but it does not authorize any other change of the account"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} RENAME TO ${user}_renamed"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} DEFAULT DATABASE ${CLICKHOUSE_DATABASE}"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'x' HOST ANY"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} VALID UNTIL '2077-01-01' ADD IDENTIFIED WITH plaintext_password BY 'x'"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} IDENTIFIED WITH plaintext_password BY 'replaced'"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user} RESET AUTHENTICATION METHODS TO NEW"

echo "-- and it does not authorize adding an authentication method to another user"
login_expect_error "human_password" "ACCESS_DENIED" "ALTER USER ${user2} ADD IDENTIFIED WITH plaintext_password BY 'x'"

echo "-- A huge default TTL saturates at the largest supported deadline instead of overflowing"
huge_valid_until=$(create_token "CREATE TOKEN SETTINGS create_token_default_ttl_seconds = 18446744073709551615" | cut -f2)
admin "SELECT toDateTime64('${huge_valid_until}', 0) = toDateTime64(253402250399, 0)"

echo "-- An unusable FORMAT is rejected before the authentication method is added"
methods_before=$(admin "SELECT length(auth_type) FROM system.users WHERE name = '${user}'")
login "human_password" "CREATE TOKEN FORMAT NoSuchFormat" 2>&1 | grep -m1 -o "UNKNOWN_FORMAT" | head -n 1
login "human_password" "CREATE TOKEN FORMAT MySQLDump" 2>&1 | grep -m1 -o "FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT" | head -n 1
login "human_password" "CREATE TOKEN SETTINGS output_format = 'NoSuchFormat'" 2>&1 | grep -m1 -o "UNKNOWN_FORMAT" | head -n 1
methods_after=$(admin "SELECT length(auth_type) FROM system.users WHERE name = '${user}'")
test "$methods_before" = "$methods_after" && echo "no authentication method added"

echo "-- CREATE TOKEN does not support the ON CLUSTER clause"
login "human_password" "CREATE TOKEN ON CLUSTER test_shard_localhost" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1

echo "-- The user still authenticates with the original password"
login "human_password" "SELECT x FROM t1"
