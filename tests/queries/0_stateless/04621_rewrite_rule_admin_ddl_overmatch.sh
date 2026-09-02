#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality
# and, for hash-equal nodes, descends only through `children`. That invariant did not hold for
# several more admin-DDL ASTs, which keep their meaning outside `children`, have a constant (or
# type-only) `getID`, and had no `updateTreeHashImpl` override:
#   * `ASTDropWorkloadQuery` (`workload_name`, `if_exists`);
#   * `ASTSetRoleQuery` (`kind`, `roles`, `to_users`);
#   * `ASTCheckGrantQuery` (the `AccessRightsElements`, which are not even an AST);
#   * `ASTMoveAccessEntityQuery` (`type`, `names`, `storage_name`);
#   * `ASTDropAccessEntityQuery` (`type`, `names`, `if_exists`, `storage_name`);
#   * `ASTCreateMaskingPolicyQuery` (the target name/table, the `UPDATE` assignments, the `WHERE`
#     condition, the `TO` roles, the priority).
# So e.g. `DROP USER a` hashed the same as `DROP USER b`, and a rule for one over-matched the
# other. The tree hash now folds those fields in. This asserts a rule fires for the exact
# statement and does NOT fire for a statement that differs only in a folded, non-`children`
# field. The non-matching statement may still fail during execution (a role that is not granted,
# a workload that does not exist, masking-policy DDL being cloud-only) — only whether the
# rewrite-rule rejection fired is asserted, since it happens before execution.
#
# Rule names are suffixed with the test database so the test stays parallel-safe despite the
# global rule registry.

DB="${CLICKHOUSE_DATABASE}"

check() # $1 rule name, $2 query, $3 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="$1" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$3: rejected"; else echo "$3: not rejected"; fi
}

check_rule_ddl_rejected() # $1 rule DDL, $2 label
{
    out=$($CLICKHOUSE_CLIENT --query "$1" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE"; then echo "$2: rejected at CREATE RULE"; else echo "$2: accepted"; fi
}

# --- DROP WORKLOAD: differ only in `workload_name` / `if_exists`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_workload_${DB} AS (DROP WORKLOAD wa_${DB}) REJECT WITH 'blocked'"
check "rule_workload_${DB}" "DROP WORKLOAD wa_${DB}" "drop workload exact"
check "rule_workload_${DB}" "DROP WORKLOAD wb_${DB}" "drop workload different name"
check "rule_workload_${DB}" "DROP WORKLOAD IF EXISTS wa_${DB}" "drop workload extra if exists"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_workload_${DB}"

# --- SET ROLE: differ only in `roles`; and the three statement kinds must not collide. ---
$CLICKHOUSE_CLIENT --query "CREATE ROLE IF NOT EXISTS ra_${DB}"
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_setrole_${DB} AS (SET ROLE ra_${DB}) REJECT WITH 'blocked'"
check "rule_setrole_${DB}" "SET ROLE ra_${DB}" "set role exact"
check "rule_setrole_${DB}" "SET ROLE rb_${DB}" "set role different role"
check "rule_setrole_${DB}" "SET ROLE DEFAULT" "set role default (different kind)"
check "rule_setrole_${DB}" "SET DEFAULT ROLE ra_${DB} TO ua_${DB}" "set default role (different kind)"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_setrole_${DB}"

# --- CHECK GRANT: differ only in the access-rights object (`AccessRightsElements`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_checkgrant_${DB} AS (CHECK GRANT SELECT ON dba_${DB}.t) REJECT WITH 'blocked'"
check "rule_checkgrant_${DB}" "CHECK GRANT SELECT ON dba_${DB}.t" "check grant exact"
check "rule_checkgrant_${DB}" "CHECK GRANT SELECT ON dbb_${DB}.t" "check grant different object"
check "rule_checkgrant_${DB}" "CHECK GRANT INSERT ON dba_${DB}.t" "check grant different access type"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_checkgrant_${DB}"

# --- MOVE USER: differ only in `names` / `storage_name` / entity type. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_move_${DB} AS (MOVE USER ua_${DB} TO local_directory) REJECT WITH 'blocked'"
check "rule_move_${DB}" "MOVE USER ua_${DB} TO local_directory" "move user exact"
check "rule_move_${DB}" "MOVE USER ub_${DB} TO local_directory" "move user different name"
check "rule_move_${DB}" "MOVE USER ua_${DB} TO memory" "move user different storage"
check "rule_move_${DB}" "MOVE ROLE ua_${DB} TO local_directory" "move role (different entity type)"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_move_${DB}"

# --- DROP USER: differ only in `names` / `if_exists` / entity type. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_dropuser_${DB} AS (DROP USER ua_${DB}) REJECT WITH 'blocked'"
check "rule_dropuser_${DB}" "DROP USER ua_${DB}" "drop user exact"
check "rule_dropuser_${DB}" "DROP USER ub_${DB}" "drop user different name"
check "rule_dropuser_${DB}" "DROP USER IF EXISTS ua_${DB}" "drop user extra if exists"
check "rule_dropuser_${DB}" "DROP ROLE ua_${DB}" "drop role (different entity type)"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_dropuser_${DB}"

# --- CREATE MASKING POLICY: differ only in the `UPDATE` assignments / `WHERE` / priority. ---
# The rewrite-rule rejection fires before execution, which is exactly what is asserted here. In
# open-source builds masking-policy DDL is disabled, so a non-matching statement then fails with
# `SUPPORT_IS_DISABLED` and creates nothing. In cloud builds masking-policy DDL is enabled, so a
# non-matching statement actually creates the policy; it is dropped below so it does not leak into
# the global `system.masking_policies` and disturb other tests.
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_masking_${DB} AS (CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 1 TO ALL) REJECT WITH 'blocked'"
check "rule_masking_${DB}" "CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 1 TO ALL" "masking policy exact"
check "rule_masking_${DB}" "CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 2 TO ALL" "masking policy different update"
check "rule_masking_${DB}" "CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 1 WHERE x != 0 TO ALL" "masking policy extra where"
check "rule_masking_${DB}" "CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 1 TO ALL PRIORITY 2" "masking policy different priority"
check "rule_masking_${DB}" "CREATE MASKING POLICY mp2_${DB} ON tbl_${DB} UPDATE x = 1 TO ALL" "masking policy different name"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_masking_${DB}"
# Drop the policies that the non-matching statements above create in cloud builds (a no-op error
# in open-source builds where the DDL is disabled).
$CLICKHOUSE_CLIENT --query "DROP MASKING POLICY IF EXISTS mp_${DB} ON tbl_${DB}" 2>/dev/null || true
$CLICKHOUSE_CLIENT --query "DROP MASKING POLICY IF EXISTS mp2_${DB} ON tbl_${DB}" 2>/dev/null || true

# A placeholder inside a masking policy's `UPDATE` / `WHERE` lives outside `children`, so the
# matcher can neither bind nor substitute it; such a rule is rejected at CREATE RULE time
# (see `forEachRewriteRuleNonChildAST`) instead of being stored and silently never firing.
check_rule_ddl_rejected "CREATE RULE rule_masking_ph_${DB} AS (CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = {v:String} TO ALL) REWRITE TO (SELECT 1)" "masking policy placeholder in update"
check_rule_ddl_rejected "CREATE RULE rule_masking_ph_${DB} AS (CREATE MASKING POLICY mp_${DB} ON tbl_${DB} UPDATE x = 1 WHERE y = {v:Int32} TO ALL) REWRITE TO (SELECT 1)" "masking policy placeholder in where"

$CLICKHOUSE_CLIENT --query "DROP ROLE IF EXISTS ra_${DB}"
