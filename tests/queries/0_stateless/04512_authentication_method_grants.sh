#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: user manipulation is not supported there
# Tag no-replicated-database: the test relies on grants on the current database, and CREATE USER is executed on all the replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_04512_${CLICKHOUSE_DATABASE}"
user2="u2_04512_${CLICKHOUSE_DATABASE}"
role="r_04512_${CLICKHOUSE_DATABASE}"

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

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}" -q "DROP USER IF EXISTS ${user2}" -q "DROP ROLE IF EXISTS ${role}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}" -q "DROP USER IF EXISTS ${user2}" -q "DROP ROLE IF EXISTS ${role}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x" -q "CREATE TABLE t2 (x UInt64) ENGINE = MergeTree ORDER BY x" -q "INSERT INTO t1 VALUES (1)" -q "INSERT INTO t2 VALUES (2)"

# The second authentication method is a 'token': it limits the access rights to a subset of the grants.
# Note that the elements without a database name must be bound to the current database.
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'full_password', plaintext_password BY 'token_password' VALID UNTIL '2077-01-01' GRANTS (SELECT ON t1, INSERT ON t1)"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${role}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t2 TO ${role}" -q "GRANT ${role} TO ${user}"

echo "-- SHOW CREATE USER shows the GRANTS clause with the database name bound"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"

echo "-- system.users exposes the grants of each authentication method"
${CLICKHOUSE_CLIENT} -q "SELECT arrayMap(x -> replaceAll(x, currentDatabase(), 'db'), auth_grants) FROM system.users WHERE name = '${user}'"

echo "-- Login with the full credential: both tables are accessible (t2 via the role)"
login "full_password" "SELECT x FROM t1"
login "full_password" "SELECT x FROM t2"

echo "-- Login with the token: t1 is accessible"
login "token_password" "SELECT x FROM t1"

echo "-- Login with the token: t2 is not accessible (the role rights are limited as well)"
login_expect_error "token_password" "ACCESS_DENIED" "SELECT x FROM t2"

echo "-- Login with the token: INSERT is listed in GRANTS but not granted to the user, so it is denied"
login_expect_error "token_password" "ACCESS_DENIED" "INSERT INTO t1 VALUES (42)"

echo "-- Login with the token: cannot grant its privileges (no grant option after the intersection)"
login_expect_error "token_password" "ACCESS_DENIED" "GRANT SELECT ON t1 TO ${user}"

echo "-- Login with the token: cannot administer roles"
login_expect_error "token_password" "ACCESS_DENIED" "GRANT ${role} TO ${user}"

echo "-- Login with the token: cannot create other tokens (no ALTER USER right)"
login_expect_error "token_password" "ACCESS_DENIED" "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'another_token' GRANTS (SELECT ON t1)"

echo "-- Reattaching to a named session with the token does not reuse the full access rights"
session="04512_session_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=full_password&session_id=${session}" -d "SELECT x FROM t2"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=token_password&session_id=${session}" -d "SELECT x FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=full_password&session_id=${session}" -d "SELECT x FROM t2"

echo "-- ALTER USER ADD IDENTIFIED with the GRANTS clause adds a new token"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'second_token' GRANTS (SELECT(x) ON ${CLICKHOUSE_DATABASE}.t2)"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"
login "second_token" "SELECT x FROM t2"
login_expect_error "second_token" "ACCESS_DENIED" "SELECT x FROM t1"

echo "-- The GRANTS clause requires a non-empty list of grants in parentheses"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS ()" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS SELECT ON t1" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1

# A GRANTS clause that grants no privileges (e.g. USAGE) is an explicit "deny everything" limit.
# It must not be treated as "no clause" (which would silently give the credential the full user rights).
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user2} IDENTIFIED WITH plaintext_password BY 'full2', plaintext_password BY 'denyall' GRANTS (USAGE ON *.*)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user2}"

echo "-- SHOW CREATE USER keeps the no-privileges clause (it does not collapse to no limit or to unparseable empty parentheses)"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user2}" | sed "s/${user2}/user2/g"

echo "-- system.users exposes USAGE ON *.* for the deny-all method"
${CLICKHOUSE_CLIENT} -q "SELECT auth_grants FROM system.users WHERE name = '${user2}'"

echo "-- The full credential can read t1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user2}&password=full2" -d "SELECT x FROM t1"

echo "-- The deny-all token cannot read t1 even though the user can"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user2}&password=denyall" -d "SELECT x FROM t1" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

# Filtered source grants (e.g. READ ON S3('...')) cannot be narrowed by the intersection, because the source
# filter is intersected as an opaque string. They are rejected explicitly instead of silently granting nothing.
echo "-- A filtered source grant in the GRANTS clause is rejected (CREATE USER)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS (READ ON S3('s3://bucket/private/.*'))" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

echo "-- A filtered source grant in the GRANTS clause is rejected (ALTER USER)"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'filtered_token' GRANTS (READ ON S3('s3://bucket/private/.*'))" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1
