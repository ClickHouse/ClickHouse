#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant did not hold for several
# access-DDL ASTs, which keep almost all of their meaning outside `children` and have a constant
# `getID`, and had no `updateTreeHashImpl` override:
#   * `ASTGrantQuery` (the `AccessRightsElements`, which are not even an AST);
#   * `ASTCreateRoleQuery` (`names`, `settings`, ...);
#   * `ASTCreateQuotaQuery` (`all_limits`, `names`, ...);
#   * `ASTCreateSettingsProfileQuery` (`settings`, `names`, ...);
#   * `ASTCreateRowPolicyQuery` (the `USING` / `WITH CHECK` filter expressions in `filters`);
#   * `ASTCreateNamedCollectionQuery` (`collection_name`, the key/value `changes`).
# So e.g. `GRANT SELECT ON a.t TO u` hashed the same as `GRANT SELECT ON b.t TO u`, and a rule for
# one over-matched the other. The tree hash now folds those fields in (for the non-AST collections
# it folds exactly the text the formatter emits). This asserts a rule fires for the exact statement
# and does NOT fire for a statement that differs only in a folded, non-`children` field.
#
# Rule names are suffixed with the test database so the test stays parallel-safe despite the global
# rule registry, and only whether the rule fired (rejection) is asserted. The exact statement is
# rejected during traversal before it executes (so nothing is created for it); the non-matching
# statement is not rejected and therefore executes, so anything it creates is dropped at the end.

DB="${CLICKHOUSE_DATABASE}"

check() # $1 rule name, $2 query, $3 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="$1" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$3: rejected"; else echo "$3: not rejected"; fi
}

$CLICKHOUSE_CLIENT --query "CREATE USER IF NOT EXISTS ug_${DB}"
$CLICKHOUSE_CLIENT --query "CREATE TABLE IF NOT EXISTS tbl_${DB} (x Int) ENGINE = Memory"

# --- GRANT: differ only in the access-rights object (`AccessRightsElements`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_grant_${DB} AS (GRANT SELECT ON dba_${DB}.t TO ug_${DB}) REJECT WITH 'blocked'"
check "rule_grant_${DB}" "GRANT SELECT ON dba_${DB}.t TO ug_${DB}" "grant exact"
check "rule_grant_${DB}" "GRANT SELECT ON dbb_${DB}.t TO ug_${DB}" "grant different object"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_grant_${DB}"

# --- CREATE ROLE: differ only in `names`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_role_${DB} AS (CREATE ROLE ra_${DB}) REJECT WITH 'blocked'"
check "rule_role_${DB}" "CREATE ROLE ra_${DB}" "role exact"
check "rule_role_${DB}" "CREATE ROLE rb_${DB}" "role different name"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_role_${DB}"

# --- CREATE QUOTA: differ only in the interval limits (`all_limits`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_quota_${DB} AS (CREATE QUOTA q_${DB} FOR INTERVAL 1 hour MAX queries = 1) REJECT WITH 'blocked'"
check "rule_quota_${DB}" "CREATE QUOTA q_${DB} FOR INTERVAL 1 hour MAX queries = 1" "quota exact"
check "rule_quota_${DB}" "CREATE QUOTA q_${DB} FOR INTERVAL 1 hour MAX queries = 2" "quota different limit"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_quota_${DB}"

# --- CREATE SETTINGS PROFILE: differ only in the settings profile element value. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_profile_${DB} AS (CREATE SETTINGS PROFILE sp_${DB} SETTINGS max_threads = 1) REJECT WITH 'blocked'"
check "rule_profile_${DB}" "CREATE SETTINGS PROFILE sp_${DB} SETTINGS max_threads = 1" "profile exact"
check "rule_profile_${DB}" "CREATE SETTINGS PROFILE sp_${DB} SETTINGS max_threads = 2" "profile different value"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_profile_${DB}"

# --- CREATE ROW POLICY: differ only in the `USING` filter expression (kept in `filters`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_policy_${DB} AS (CREATE ROW POLICY rp_${DB} ON tbl_${DB} USING x = 1) REJECT WITH 'blocked'"
check "rule_policy_${DB}" "CREATE ROW POLICY rp_${DB} ON tbl_${DB} USING x = 1" "policy exact"
check "rule_policy_${DB}" "CREATE ROW POLICY rp_${DB} ON tbl_${DB} USING x = 2" "policy different filter"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_policy_${DB}"

# --- CREATE NAMED COLLECTION: differ only in the key/value `changes`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_nc_${DB} AS (CREATE NAMED COLLECTION nc_${DB} AS a = 1) REJECT WITH 'blocked'"
check "rule_nc_${DB}" "CREATE NAMED COLLECTION nc_${DB} AS a = 1" "named collection exact"
check "rule_nc_${DB}" "CREATE NAMED COLLECTION nc_${DB} AS a = 2" "named collection different value"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_nc_${DB}"

# Drop what the non-matching (executed) statements created.
$CLICKHOUSE_CLIENT --query "DROP ROLE IF EXISTS rb_${DB}"
$CLICKHOUSE_CLIENT --query "DROP QUOTA IF EXISTS q_${DB}"
$CLICKHOUSE_CLIENT --query "DROP SETTINGS PROFILE IF EXISTS sp_${DB}"
$CLICKHOUSE_CLIENT --query "DROP ROW POLICY IF EXISTS rp_${DB} ON tbl_${DB}"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS nc_${DB}"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl_${DB}"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ug_${DB}"
