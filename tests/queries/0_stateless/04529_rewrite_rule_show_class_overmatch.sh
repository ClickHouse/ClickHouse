#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: the rewrite-rule matcher treats an equal `getTreeHash(true)` as semantic equality and,
# for hash-equal nodes, descends only through `children`. That invariant does not hold for every query
# type a rule template accepts. Several more `SHOW` AST classes kept distinguishing state outside both
# `children` and `updateTreeHashImpl`:
#   * `ASTShowColumnsQuery`  -- `extended` / `full`, the `like` pattern (with `not_like` /
#     `case_insensitive_like`), the `database` / `table`, and the `where` / `limit` clauses.
#   * `ASTShowIndexesQuery`  -- the `database` / `table` / `where` clause; and its `getID` even
#     returned the constant `"ShowColumns"`, so `SHOW INDEXES` could collide with `SHOW COLUMNS`.
#   * `ASTShowFunctionsQuery` -- the `like` pattern and its `case_insensitive_like` modifier.
#   * `ASTShowSettingQuery`  -- the selected setting name.
# So e.g. a rule for `SHOW SETTING max_threads` also matched `SHOW SETTING max_memory_usage`, and a
# `SHOW INDEXES` rule over-matched a `SHOW COLUMNS` query. The tree hashes now fold those fields in
# (and `ASTShowIndexesQuery::getID` is corrected), so a rule for one statement no longer over-matches
# the other. The rule names are suffixed with the test database so the test stays parallel-safe despite
# the global rule registry, and only whether the rule fired (rejection) is asserted, so the `SHOW`
# statements themselves need not actually run.

RULE_SETTING="rule_show_setting_${CLICKHOUSE_DATABASE}"
RULE_INDEXES="rule_show_indexes_${CLICKHOUSE_DATABASE}"
RULE_COLUMNS="rule_show_columns_${CLICKHOUSE_DATABASE}"
RULE_COLUMNS_ILIKE="rule_show_columns_ilike_${CLICKHOUSE_DATABASE}"
RULE_FUNCTIONS="rule_show_functions_${CLICKHOUSE_DATABASE}"

check() { # $1 = label, $2 = output; prints whether the rule fired
    if echo "$2" | grep -q "REWRITE_RULE_REJECTION"; then echo "$1: rejected"; else echo "$1: not rejected"; fi
}

# --- SHOW SETTING: differ only in the selected setting name (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_SETTING} AS (SHOW SETTING max_threads) REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_SETTING}" --query "SHOW SETTING max_threads" 2>&1 || true)
check "setting exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_SETTING}" --query "SHOW SETTING max_memory_usage" 2>&1 || true)
check "setting other" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_SETTING}"

# --- SHOW INDEXES vs SHOW COLUMNS: same table, but `getID` used to be `"ShowColumns"` for both. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_INDEXES} AS (SHOW INDEXES FROM tbl) REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_INDEXES}" --query "SHOW INDEXES FROM tbl" 2>&1 || true)
check "indexes exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_INDEXES}" --query "SHOW COLUMNS FROM tbl" 2>&1 || true)
check "indexes vs columns" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_INDEXES}"

# --- SHOW COLUMNS: differ only in the `like` pattern (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_COLUMNS} AS (SHOW COLUMNS FROM tbl LIKE 'a%') REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLUMNS}" --query "SHOW COLUMNS FROM tbl LIKE 'a%'" 2>&1 || true)
check "columns exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLUMNS}" --query "SHOW COLUMNS FROM tbl LIKE 'b%'" 2>&1 || true)
check "columns other pattern" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_COLUMNS}"

# --- SHOW COLUMNS: `ILIKE ''` sets `case_insensitive_like` while leaving `like` empty; it must not
# over-match a plain `SHOW COLUMNS`. This also exercises the format -> parse round-trip of the empty
# `ILIKE` pattern that the debug-build tree-hash consistency check relies on. ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_COLUMNS_ILIKE} AS (SHOW COLUMNS FROM tbl ILIKE '') REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLUMNS_ILIKE}" --query "SHOW COLUMNS FROM tbl ILIKE ''" 2>&1 || true)
check "columns ilike-empty exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_COLUMNS_ILIKE}" --query "SHOW COLUMNS FROM tbl" 2>&1 || true)
check "columns ilike-empty vs plain" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_COLUMNS_ILIKE}"

# --- SHOW FUNCTIONS: differ only in the `like` pattern (kept outside `children`). ---
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_FUNCTIONS} AS (SHOW FUNCTIONS LIKE 'a%') REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_FUNCTIONS}" --query "SHOW FUNCTIONS LIKE 'a%'" 2>&1 || true)
check "functions exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_FUNCTIONS}" --query "SHOW FUNCTIONS LIKE 'b%'" 2>&1 || true)
check "functions other pattern" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_FUNCTIONS}"
