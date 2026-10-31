#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The matcher hashes settings-profile subtrees and role sets in access-entity DDL, although the
# parsers keep them outside `children`. Rule-template limits must cover those semantic carriers.
ACTIVE="rule_access_limits_${CLICKHOUSE_DATABASE}"
ROLE="rr_access_role_${CLICKHOUSE_DATABASE}"
PROFILE="rr_access_profile_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${ACTIVE}"
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${ROLE}"
    $CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${PROFILE}"
}

trap cleanup EXIT
cleanup
$CLICKHOUSE_CLIENT --query "CREATE RULE ${ACTIVE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

check() # $1 label, $2 template
{
    echo "$1:"
    $CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements 4 --query \
        "CREATE RULE ${ROLE} AS ($2) REJECT WITH 'blocked'" 2>&1 | grep -o -m1 "TOO_BIG_AST" || echo "not rejected"
}

check "CREATE ROLE settings" "CREATE ROLE ${ROLE} SETTINGS max_threads = 1, max_memory_usage = 2"
check "CREATE SETTINGS PROFILE settings and roles" "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS max_threads = 1, max_memory_usage = 2 TO ${ROLE}"
