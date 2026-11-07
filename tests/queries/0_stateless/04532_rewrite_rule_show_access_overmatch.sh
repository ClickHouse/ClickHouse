#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant did not hold for the access
# `SHOW` classes reachable through `ParserQueryWithOutput`, which keep their whole meaning outside
# `children`:
#   * `ASTShowAccessEntitiesQuery` (`SHOW ROW POLICIES p`): `short_name` / `database_and_table_name`.
#   * `ASTShowCreateAccessEntityQuery` (`SHOW CREATE USER u`, `SHOW CREATE ROW POLICY p ON db.t`):
#     `names` and the nested `row_policy_names` (`ASTRowPolicyNames::full_names`).
#   * `ASTShowGrantsQuery` (`SHOW GRANTS ... FINAL`, `SHOW GRANTS FOR u`): `final` and the nested
#     `for_roles` (`ASTRolesOrUsersSet::names`).
# So e.g. `SHOW CREATE USER a` hashed the same as `SHOW CREATE USER b`, and a rule for one
# over-matched the other. The tree hash now folds those fields (and the nested `ASTRolesOrUsersSet` /
# `ASTRowPolicyNames` fold their own state) in. The rule names are suffixed with the test database so
# the test stays parallel-safe despite the global rule registry, and only whether the rule fired
# (rejection) is asserted, so the referenced entities need not actually exist (REJECT fires during
# traversal, before name resolution).

RULE_POLICIES="rule_show_policies_overmatch_${CLICKHOUSE_DATABASE}"
RULE_GRANTS_FINAL="rule_show_grants_final_overmatch_${CLICKHOUSE_DATABASE}"
RULE_GRANTS_FOR="rule_show_grants_for_overmatch_${CLICKHOUSE_DATABASE}"
RULE_CREATE_USER="rule_show_create_user_overmatch_${CLICKHOUSE_DATABASE}"
RULE_CREATE_POLICY="rule_show_create_policy_overmatch_${CLICKHOUSE_DATABASE}"

check() # $1 rule name, $2 query, $3 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="$1" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$3: rejected"; else echo "$3: not rejected"; fi
}

# --- SHOW ROW POLICIES p1 vs p2: differ only in `short_name` (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_POLICIES} AS (SHOW ROW POLICIES p_secret) REJECT WITH 'blocked'"
check "${RULE_POLICIES}" "SHOW ROW POLICIES p_secret" "show policies exact"
check "${RULE_POLICIES}" "SHOW ROW POLICIES p_other" "show policies other name"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_POLICIES}"

# --- SHOW GRANTS FINAL vs SHOW GRANTS: differ only in the `final` flag. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_GRANTS_FINAL} AS (SHOW GRANTS FINAL) REJECT WITH 'blocked'"
check "${RULE_GRANTS_FINAL}" "SHOW GRANTS FINAL" "show grants final exact"
check "${RULE_GRANTS_FINAL}" "SHOW GRANTS" "show grants without final"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_GRANTS_FINAL}"

# --- SHOW GRANTS FOR u_secret vs FOR u_other: differ only in the nested `for_roles` names. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_GRANTS_FOR} AS (SHOW GRANTS FOR u_secret) REJECT WITH 'blocked'"
check "${RULE_GRANTS_FOR}" "SHOW GRANTS FOR u_secret" "show grants for exact"
check "${RULE_GRANTS_FOR}" "SHOW GRANTS FOR u_other" "show grants for other name"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_GRANTS_FOR}"

# --- SHOW CREATE USER a vs b: differ only in `names`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_CREATE_USER} AS (SHOW CREATE USER u_secret) REJECT WITH 'blocked'"
check "${RULE_CREATE_USER}" "SHOW CREATE USER u_secret" "show create user exact"
check "${RULE_CREATE_USER}" "SHOW CREATE USER u_other" "show create user other name"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_CREATE_USER}"

# --- SHOW CREATE ROW POLICY p_secret ON db.t vs p_other ON db.t: differ only in the nested
#     `row_policy_names`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_CREATE_POLICY} AS (SHOW CREATE ROW POLICY p_secret ON db.t) REJECT WITH 'blocked'"
check "${RULE_CREATE_POLICY}" "SHOW CREATE ROW POLICY p_secret ON db.t" "show create policy exact"
check "${RULE_CREATE_POLICY}" "SHOW CREATE ROW POLICY p_other ON db.t" "show create policy other name"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_CREATE_POLICY}"
