#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE RULE`, `ALTER RULE` and `DROP RULE` each check their own grant
# (`AccessType::CREATE_RULE` / `ALTER_RULE` / `DROP_RULE`) before doing anything. A user
# holding none of them must get `ACCESS_DENIED` from the DDL entrypoint itself, and a user
# holding one of them must still be denied the other two - otherwise a wiring regression that
# made all three consult the same privilege, or dropped a check entirely, would go unnoticed.
#
# The access check runs before the rule is looked up, so `ALTER`/`DROP` of an existing rule is
# denied for the name's sake and not for the rule's - each denial below therefore has a
# matching success once, and only once, the corresponding grant is added.

RULE="rule_denied_${CLICKHOUSE_DATABASE}"
RULE_CREATED="rule_denied_created_${CLICKHOUSE_DATABASE}"
USER="user_denied_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} NOT IDENTIFIED"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 2)"

run_as_user()
{
    $CLICKHOUSE_CLIENT --user "${USER}" --query "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo "OK"
}

echo "no grants:"
run_as_user "CREATE RULE ${RULE_CREATED} AS (SELECT '${RULE_CREATED}') REWRITE TO (SELECT 3)"
run_as_user "ALTER RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 4)"
run_as_user "DROP RULE ${RULE}"

# Each grant unlocks its own statement and nothing else.
$CLICKHOUSE_CLIENT --query "GRANT CREATE RULE ON *.* TO ${USER}"
echo "with CREATE RULE:"
run_as_user "CREATE RULE ${RULE_CREATED} AS (SELECT '${RULE_CREATED}') REWRITE TO (SELECT 3)"
run_as_user "ALTER RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 4)"
run_as_user "DROP RULE ${RULE}"

$CLICKHOUSE_CLIENT --query "GRANT ALTER RULE ON *.* TO ${USER}"
echo "with ALTER RULE:"
run_as_user "ALTER RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 4)"
run_as_user "DROP RULE ${RULE}"

$CLICKHOUSE_CLIENT --query "GRANT DROP RULE ON *.* TO ${USER}"
echo "with DROP RULE:"
run_as_user "DROP RULE ${RULE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_CREATED}"
$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
