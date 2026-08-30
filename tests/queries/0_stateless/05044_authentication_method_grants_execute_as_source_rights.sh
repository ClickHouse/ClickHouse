#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: user manipulation is not supported there
# Tag no-replicated-database: the test relies on grants on the current database, and CREATE USER is executed on all the replicas

# The symmetric case of `04816_authentication_method_grants_execute_as`: the impersonation target
# has a privilege that the token lists but the impersonating user was never granted directly.
#
# `EXECUTE AS` switches the principal, so the impersonated context runs with the target's access
# rights, intersected with the `GRANTS` of the authentication method - the credential limit is never
# shed. The limit is a ceiling over the impersonated principal, not an additional grant: a token can
# do nothing here that the same user cannot do with an unlimited credential, because impersonation
# requires `IMPERSONATE ON target` to be both granted to the user and listed in the method. The test
# pins exactly that: the token behaves like the user's full credential, and a privilege of the target
# that the method does not list stays unavailable to the token.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_05044_${CLICKHOUSE_DATABASE}"
target="t_05044_${CLICKHOUSE_DATABASE}"

function admin()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

function cleanup()
{
    admin "DROP USER IF EXISTS ${user}, ${user}_2, ${target}"
}

cleanup

admin "CREATE TABLE t1 (x UInt64) ENGINE = Memory"
admin "CREATE TABLE t2 (x UInt64) ENGINE = Memory"
admin "CREATE TABLE t3 (x UInt64) ENGINE = Memory"
admin "INSERT INTO t1 VALUES (1)"
admin "INSERT INTO t2 VALUES (2)"
admin "INSERT INTO t3 VALUES (3)"

# The target holds every privilege of interest, including the one the token does not list.
admin "CREATE USER ${target} IDENTIFIED WITH no_password"
admin "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${target}"

# The user itself may read only `t1`; `t2` is listed by the token but never granted to the user.
admin "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'full_password', plaintext_password BY 'token_password' GRANTS (SELECT ON t1, SELECT ON t2, IMPERSONATE ON ${target})"
admin "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user}"
admin "GRANT IMPERSONATE ON ${target} TO ${user}"

echo "-- the user itself cannot read the table it does not hold, listed by the token or not"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "SELECT sum(x) FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "SELECT sum(x) FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

echo "-- impersonation runs as the target: the listed privilege of the target is available"
echo "-- to the token exactly as it is to the user's unlimited credential"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target} SELECT sum(x) FROM t2"
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "EXECUTE AS ${target} SELECT sum(x) FROM t2"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target}; SELECT sum(x) FROM t2;"
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "EXECUTE AS ${target}; SELECT sum(x) FROM t2;"

echo "-- a privilege of the target that the token does not list stays denied to the token"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target} SELECT sum(x) FROM t3" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target}; SELECT sum(x) FROM t3;" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

echo "-- and is available to the unlimited credential"
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "EXECUTE AS ${target} SELECT sum(x) FROM t3"

echo "-- impersonation itself requires the privilege to be both granted and listed"
admin "CREATE USER ${user}_2 IDENTIFIED WITH plaintext_password BY 'token_password' GRANTS (SELECT ON t2)"
admin "GRANT IMPERSONATE ON ${target} TO ${user}_2"
$CLICKHOUSE_CLIENT --user "${user}_2" --password "token_password" --query "EXECUTE AS ${target} SELECT sum(x) FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

cleanup
