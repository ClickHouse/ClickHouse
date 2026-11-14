#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant did not hold for
# `ASTCreateUserQuery`, which keeps almost all of its meaning outside `children` (only the
# authentication methods and `VALID UNTIL` go into `children`) and has a constant `getID`:
#   * the target user `names`;
#   * the `HOST` descriptors (`AllowedClientHosts`);
#   * the `DEFAULT DATABASE` (`ASTDatabaseOrNone`);
#   * the `SETTINGS` (`ASTSettingsProfileElements` / `ASTSettingsProfileElement`);
#   * `DEFAULT ROLE` / `GRANTEES` (`ASTRolesOrUsersSet`).
# So e.g. `CREATE USER a` hashed the same as `CREATE USER b`, and a rule for one over-matched the
# other. The tree hash now folds those fields in (and the nested `ASTDatabaseOrNone` /
# settings-profile-element classes fold their own state). A presence flag is folded before each
# optional member, so `... GRANTEES NONE` and `... DEFAULT ROLE NONE` (a single, identical
# `ASTRolesOrUsersSet` kept in a different member) no longer collide either.
#
# The rule names are suffixed with the test database so the test stays parallel-safe despite the
# global rule registry, and only whether the rule fired (rejection) is asserted. The exact query is
# rejected during traversal before it executes (so no user is created for it); the non-matching
# query is not rejected and therefore executes, so its user is dropped at the end.

DB="${CLICKHOUSE_DATABASE}"

check() # $1 rule name, $2 query, $3 label
{
    out=$($CLICKHOUSE_CLIENT --query_rules="$1" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$3: rejected"; else echo "$3: not rejected"; fi
}

# --- CREATE USER a vs b: differ only in `names`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_cu_names_${DB} AS (CREATE USER u1a_${DB}) REJECT WITH 'blocked'"
check "rule_cu_names_${DB}" "CREATE USER u1a_${DB}" "names exact"
check "rule_cu_names_${DB}" "CREATE USER u1b_${DB}" "names other"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_cu_names_${DB}"

# --- HOST LOCAL vs HOST IP: differ only in the `AllowedClientHosts`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_cu_host_${DB} AS (CREATE USER u2_${DB} HOST LOCAL) REJECT WITH 'blocked'"
check "rule_cu_host_${DB}" "CREATE USER u2_${DB} HOST LOCAL" "host exact"
check "rule_cu_host_${DB}" "CREATE USER u2_${DB} HOST IP '1.2.3.4'" "host different"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_cu_host_${DB}"

# --- DEFAULT DATABASE dba vs dbb: differ only in the `ASTDatabaseOrNone`. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_cu_db_${DB} AS (CREATE USER u3_${DB} DEFAULT DATABASE dba) REJECT WITH 'blocked'"
check "rule_cu_db_${DB}" "CREATE USER u3_${DB} DEFAULT DATABASE dba" "default database exact"
check "rule_cu_db_${DB}" "CREATE USER u3_${DB} DEFAULT DATABASE dbb" "default database different"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_cu_db_${DB}"

# --- SETTINGS max_threads = 1 vs 2: differ only in the settings profile element value. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_cu_settings_${DB} AS (CREATE USER u4_${DB} SETTINGS max_threads = 1) REJECT WITH 'blocked'"
check "rule_cu_settings_${DB}" "CREATE USER u4_${DB} SETTINGS max_threads = 1" "settings exact"
check "rule_cu_settings_${DB}" "CREATE USER u4_${DB} SETTINGS max_threads = 2" "settings different value"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_cu_settings_${DB}"

# --- GRANTEES NONE vs DEFAULT ROLE NONE: identical `ASTRolesOrUsersSet`, different member. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE rule_cu_pos_${DB} AS (CREATE USER u5_${DB} GRANTEES NONE) REJECT WITH 'blocked'"
check "rule_cu_pos_${DB}" "CREATE USER u5_${DB} GRANTEES NONE" "position exact"
check "rule_cu_pos_${DB}" "CREATE USER u5_${DB} DEFAULT ROLE NONE" "position different member"
$CLICKHOUSE_CLIENT --query "DROP RULE rule_cu_pos_${DB}"

# Drop the users that the non-matching queries may have created.
for u in u1b_${DB} u2_${DB} u3_${DB} u4_${DB} u5_${DB}; do
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${u}"
done
