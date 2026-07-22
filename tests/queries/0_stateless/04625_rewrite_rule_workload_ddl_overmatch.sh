#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality
# and, for hash-equal nodes, descends only through `children`. That invariant did not hold for
# three more DDL ASTs, which keep part of their meaning outside `children` and had no
# `updateTreeHashImpl` override:
#   * `ASTAlterNamedCollectionQuery` (`collection_name`, `changes`, `delete_keys`, `if_exists`;
#     constant `getID`, no `children` at all);
#   * `ASTCreateWorkloadQuery` (`changes`, `or_replace`, `if_not_exists`; the name and the parent
#     are `children`, so only the rest collided);
#   * `ASTCreateResourceQuery` (`operations`, `or_replace`, `if_not_exists`; the name is a child).
# So e.g. `ALTER NAMED COLLECTION a SET x = 1` hashed the same as `ALTER NAMED COLLECTION b
# DELETE y`, and a rule for one over-matched the other. The tree hash now folds those fields in.
# This asserts a rule fires for the exact statement and does NOT fire for a statement that
# differs only in a folded, non-`children` field. Only whether the rewrite-rule rejection fired
# is asserted, since it happens before execution.
#
# Workload entities are global and a parentless workload becomes the single allowed root, so
# every `CREATE WORKLOAD` here uses `IN <nonexistent parent>`: the non-matching statements then
# fail reference validation ("references another workload entity that doesn't exist") before any
# entity is created, leaving no global state behind and no root-workload conflict with parallel
# tests. The non-matching `CREATE RESOURCE` statements do execute, so they use fake DB-suffixed
# disk names (like `03232_workloads_and_resources`) and are dropped below; the `ANY DISK` form,
# which could claim every disk, is only ever the rejected exact match, never executed.
#
# Rule names are suffixed with the test database so the test stays parallel-safe despite the
# global rule registry.

DB="${CLICKHOUSE_DATABASE}"

check() # $1 rule name, $2 query, $3 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="$1" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$3: rejected"; else echo "$3: not rejected"; fi
}

# --- ALTER NAMED COLLECTION: differ only in name / changes / delete keys / `IF EXISTS`. ---
# The non-matching statements fail at execution (the collection does not exist) with no side
# effect; the `IF EXISTS` form is a no-op.
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_alter_nc_${DB} AS (ALTER NAMED COLLECTION nca_${DB} SET k1 = 1) REJECT WITH 'blocked'"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION nca_${DB} SET k1 = 1" "alter named collection exact"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION ncb_${DB} SET k1 = 1" "alter named collection different name"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION nca_${DB} SET k1 = 2" "alter named collection different value"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION nca_${DB} SET k2 = 1" "alter named collection different key"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION nca_${DB} SET k1 = 1 OVERRIDABLE" "alter named collection extra overridable"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION IF EXISTS nca_${DB} SET k1 = 1" "alter named collection extra if exists"
check "rule_alter_nc_${DB}" "ALTER NAMED COLLECTION nca_${DB} DELETE k1" "alter named collection delete instead of set"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_alter_nc_${DB}"

# --- CREATE WORKLOAD: differ only in settings / `OR REPLACE` / `IF NOT EXISTS`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_create_wl_${DB} AS (CREATE WORKLOAD wa_${DB} IN npar_${DB}) REJECT WITH 'blocked'"
check "rule_create_wl_${DB}" "CREATE WORKLOAD wa_${DB} IN npar_${DB}" "create workload exact"
check "rule_create_wl_${DB}" "CREATE WORKLOAD wb_${DB} IN npar_${DB}" "create workload different name"
check "rule_create_wl_${DB}" "CREATE WORKLOAD IF NOT EXISTS wa_${DB} IN npar_${DB}" "create workload extra if not exists"
check "rule_create_wl_${DB}" "CREATE OR REPLACE WORKLOAD wa_${DB} IN npar_${DB}" "create workload extra or replace"
check "rule_create_wl_${DB}" "CREATE WORKLOAD wa_${DB} IN npar_${DB} SETTINGS max_io_requests = 100" "create workload extra settings"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_create_wl_${DB}"

$CLICKHOUSE_CLIENT --query "CREATE RULE rule_create_wls_${DB} AS (CREATE WORKLOAD wa_${DB} IN npar_${DB} SETTINGS max_io_requests = 100) REJECT WITH 'blocked'"
check "rule_create_wls_${DB}" "CREATE WORKLOAD wa_${DB} IN npar_${DB} SETTINGS max_io_requests = 100" "create workload settings exact"
check "rule_create_wls_${DB}" "CREATE WORKLOAD wa_${DB} IN npar_${DB} SETTINGS max_io_requests = 200" "create workload different setting value"
check "rule_create_wls_${DB}" "CREATE WORKLOAD wa_${DB} IN npar_${DB} SETTINGS max_io_requests = 100 FOR res_${DB}" "create workload extra for resource"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_create_wls_${DB}"

# --- CREATE RESOURCE: differ only in operations / `OR REPLACE` / `IF NOT EXISTS`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_create_res_${DB} AS (CREATE RESOURCE ra_${DB} (READ DISK da_${DB})) REJECT WITH 'blocked'"
check "rule_create_res_${DB}" "CREATE RESOURCE ra_${DB} (READ DISK da_${DB})" "create resource exact"
check "rule_create_res_${DB}" "CREATE RESOURCE rb_${DB} (READ DISK da_${DB})" "create resource different name"
check "rule_create_res_${DB}" "CREATE RESOURCE ra_${DB} (WRITE DISK da_${DB})" "create resource different mode"
check "rule_create_res_${DB}" "CREATE RESOURCE ra_${DB} (READ DISK db_${DB})" "create resource different disk"
check "rule_create_res_${DB}" "CREATE RESOURCE ra_${DB} (READ DISK da_${DB}, WRITE DISK da_${DB})" "create resource extra operation"
check "rule_create_res_${DB}" "CREATE RESOURCE IF NOT EXISTS ra_${DB} (READ DISK da_${DB})" "create resource extra if not exists"
check "rule_create_res_${DB}" "CREATE OR REPLACE RESOURCE ra_${DB} (READ DISK da_${DB})" "create resource extra or replace"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_create_res_${DB}"

# `ANY DISK` (the optional disk is absent) must not collide with a specific disk. The `ANY DISK`
# form is the rule's exact match, so it is rejected and never executed; the specific-disk form is
# the non-matching statement and only creates a fake-disk resource that is dropped below.
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_create_resany_${DB} AS (CREATE RESOURCE rc_${DB} (READ ANY DISK)) REJECT WITH 'blocked'"
check "rule_create_resany_${DB}" "CREATE RESOURCE rc_${DB} (READ ANY DISK)" "create resource any disk exact"
check "rule_create_resany_${DB}" "CREATE RESOURCE rc_${DB} (READ DISK da_${DB})" "create resource specific disk"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_create_resany_${DB}"

# Drop the fake-disk resources that the non-matching statements above actually create.
$CLICKHOUSE_CLIENT --query "DROP RESOURCE IF EXISTS ra_${DB}"
$CLICKHOUSE_CLIENT --query "DROP RESOURCE IF EXISTS rb_${DB}"
$CLICKHOUSE_CLIENT --query "DROP RESOURCE IF EXISTS rc_${DB}"
