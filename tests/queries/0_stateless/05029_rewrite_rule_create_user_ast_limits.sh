#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ASTCreateUserQuery` keeps `SETTINGS`, `DEFAULT ROLE`, `GRANTEES` and `DEFAULT DATABASE` outside
# `children` while the matcher folds all of them into the tree hash, so the rule-template AST limits
# must see those subtrees too.
ACTIVE="rule_user_limits_${CLICKHOUSE_DATABASE}"
RULE="rr_user_rule_${CLICKHOUSE_DATABASE}"
USER="rr_user_${CLICKHOUSE_DATABASE}"
ROLE="rr_user_role_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${ACTIVE}"
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
}

trap cleanup EXIT
cleanup
$CLICKHOUSE_CLIENT --query "CREATE RULE ${ACTIVE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

# `CREATE USER u` alone counts as exactly 5 AST elements for the rule-template walk (the rule
# node, the `CREATE USER` node, `names`, the name-with-host and the name itself), so a limit of 5
# passes the bare statement and only the extra clauses below can trip it.
check() # $1 label, $2 template
{
    echo "$1:"
    $CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements 5 --query \
        "CREATE RULE ${RULE} AS ($2) REJECT WITH 'blocked'" 2>&1 | grep -o -m1 "TOO_BIG_AST" || echo "not rejected"
    # A template that fits the limit is actually created, and rules are global server state.
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
}

check "bare CREATE USER (baseline, must fit)" "CREATE USER ${USER}"
check "CREATE USER settings" "CREATE USER ${USER} SETTINGS max_threads = 1, max_memory_usage = 2"
check "ALTER USER settings" "ALTER USER ${USER} SETTINGS max_threads = 1, max_memory_usage = 2"
check "CREATE USER default role" "CREATE USER ${USER} DEFAULT ROLE ${ROLE}, ${ROLE}_2, ${ROLE}_3, ${ROLE}_4, ${ROLE}_5"
check "CREATE USER grantees" "CREATE USER ${USER} GRANTEES ${ROLE}, ${ROLE}_2, ${ROLE}_3, ${ROLE}_4, ${ROLE}_5"
check "CREATE USER default database" "CREATE USER ${USER} DEFAULT DATABASE ${CLICKHOUSE_DATABASE}"
