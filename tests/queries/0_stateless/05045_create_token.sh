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

echo "-- The token is a 32-character alphanumeric string, and a new one is generated every time"
token=$(login "human_password" "CREATE TOKEN FORMAT TSVRaw")
another_token=$(login "human_password" "CREATE TOKEN FORMAT TSVRaw")
is_token "$token"
is_token "$another_token"
test "$token" != "$another_token" && echo "tokens differ"

echo "-- The token authenticates as the user, with the full rights of the user"
login "$token" "SELECT currentUser()" | sed "s/${user}/user/g"
login "$token" "SELECT x FROM t1"
login "$token" "SELECT x FROM t2"

echo "-- The GRANTS clause limits the rights of the sessions authenticated with the token"
limited_token=$(login "human_password" "CREATE TOKEN GRANTS (SELECT ON ${CLICKHOUSE_DATABASE}.t1) FORMAT TSVRaw")
login "$limited_token" "SELECT x FROM t1"
login_expect_error "$limited_token" "ACCESS_DENIED" "SELECT x FROM t2"

echo "-- The VALID UNTIL clause is stored with the authentication method"
dated_token=$(login "human_password" "CREATE TOKEN VALID UNTIL '2077-01-01 00:00:00' GRANTS (SELECT ON ${CLICKHOUSE_DATABASE}.t2) FORMAT TSVRaw")
login "$dated_token" "SELECT x FROM t2"

echo "-- The generated secrets are stored hashed and are not shown by SHOW CREATE USER"
admin "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"

echo "-- An already expired token does not authenticate"
expired_token=$(login "human_password" "CREATE TOKEN VALID FOR INTERVAL -1 DAY FORMAT TSVRaw")
login_expect_error "$expired_token" "AUTHENTICATION_FAILED" "SELECT 1"

echo "-- A session authenticated with an unlimited token can create tokens"
is_token "$(login "$token" "CREATE TOKEN FORMAT TSVRaw")"

echo "-- A session limited by the GRANTS clause cannot, even when CREATE TOKEN is listed and granted"
minting_token=$(login "human_password" "CREATE TOKEN GRANTS (CREATE TOKEN ON *.*) FORMAT TSVRaw")
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

echo "-- CREATE TOKEN does not support the ON CLUSTER clause"
login "human_password" "CREATE TOKEN ON CLUSTER test_shard_localhost" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1

echo "-- The user still authenticates with the original password"
login "human_password" "SELECT x FROM t1"
