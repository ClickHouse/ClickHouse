#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: user manipulation is not supported there
# Tag no-replicated-database: the test relies on grants on the current database, and CREATE USER is executed on all the replicas

# The GRANTS clause of an authentication method limits the session's access rights to the
# intersection with the listed grants. The limit is a property of the credential, not of the
# principal, so `EXECUTE AS` (both the subquery form and the session form) must not shed it:
# a token listing `IMPERSONATE ON target` may impersonate, but the impersonated context stays
# limited to the intersection of the target's rights with the token's listed grants.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_04816_${CLICKHOUSE_DATABASE}"
target="t_04816_${CLICKHOUSE_DATABASE}"

function admin()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$1"
}

function cleanup()
{
    admin "DROP USER IF EXISTS ${user}, ${target}"
}

cleanup

admin "CREATE TABLE t1 (x UInt64) ENGINE = Memory"
admin "CREATE TABLE t2 (x UInt64) ENGINE = Memory"
admin "INSERT INTO t1 VALUES (1)"
admin "INSERT INTO t2 VALUES (2)"

admin "CREATE USER ${target} IDENTIFIED WITH no_password"
admin "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${target}"

admin "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'full_password', plaintext_password BY 'token_password' GRANTS (SELECT ON t1, IMPERSONATE ON ${target})"
admin "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user}"
admin "GRANT IMPERSONATE ON ${target} TO ${user}"

echo "-- token cannot read the unlisted table itself"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "SELECT count() FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

echo "-- token, subquery form, listed table"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target} SELECT count() FROM t1"

echo "-- token, subquery form, unlisted table"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target} SELECT count() FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

echo "-- token, session form, listed table"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target}; SELECT count() FROM t1;"

echo "-- token, session form, unlisted table"
$CLICKHOUSE_CLIENT --user "${user}" --password "token_password" --query "EXECUTE AS ${target}; SELECT count() FROM t2;" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

echo "-- full credential, subquery form, unlisted table"
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "EXECUTE AS ${target} SELECT count() FROM t2"

echo "-- full credential, session form, unlisted table"
$CLICKHOUSE_CLIENT --user "${user}" --password "full_password" --query "EXECUTE AS ${target}; SELECT count() FROM t2;"

cleanup
