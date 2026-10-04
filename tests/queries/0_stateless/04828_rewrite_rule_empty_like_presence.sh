#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression: `LIKE ''` is a valid clause with an empty pattern, but the affected ASTs represented
# a LIKE clause only by the pattern string plus the `not_like` / `case_insensitive_like` modifier
# flags. A plain empty-pattern `LIKE ''` therefore hashed and formatted exactly like the query
# without the clause, so e.g. `SHOW TABLES LIKE ''` and `SHOW TABLES` shared one tree hash and a
# rewrite/reject rule written for one form also matched the other. The same collapse existed in
# `ASTShowColumnsQuery`, `ASTShowFunctionsQuery`, and `ASTDropQuery` (`TRUNCATE TABLES FROM ...
# LIKE ''`), and the AST JSON reader rejected the round-trip of `NOT LIKE ''` / `ILIKE ''` because
# it required a non-empty pattern for the modifier flags. The ASTs now carry an explicit `has_like`
# presence bit that is folded into the tree hash, emitted by the formatter, and serialized in JSON.

# --- The formatter preserves the empty-pattern clause (escaping-agnostic boolean checks). ---
$CLICKHOUSE_CLIENT --query "SELECT endsWith(formatQuery('SHOW TABLES LIKE '''''), 'LIKE ''''')"
$CLICKHOUSE_CLIENT --query "SELECT endsWith(formatQuery('SHOW DATABASES LIKE '''''), 'LIKE ''''')"
$CLICKHOUSE_CLIENT --query "SELECT endsWith(formatQuery('SHOW COLUMNS FROM tbl LIKE '''''), 'LIKE ''''')"
$CLICKHOUSE_CLIENT --query "SELECT endsWith(formatQuery('SHOW FUNCTIONS LIKE '''''), 'LIKE ''''')"
$CLICKHOUSE_CLIENT --query "SELECT endsWith(formatQuery('TRUNCATE TABLES FROM db LIKE '''''), 'LIKE ''''')"

# --- The AST JSON round-trip preserves the clause, including the modifier-only forms it used to reject. ---
$CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES LIKE '''''))"
$CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES NOT LIKE '''''))"
$CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES ILIKE '''''))"
$CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON(parseQueryToJSON('SHOW COLUMNS FROM tbl NOT LIKE '''''))"
$CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON(parseQueryToJSON('TRUNCATE TABLES FROM db NOT LIKE '''''))"

# --- Executing the query exercises the debug-build parse -> format -> parse tree-hash check.
# (The empty pattern is treated as "no filter" by the interpreter, and the test database is empty,
# so the query prints nothing. `SHOW FUNCTIONS LIKE ''` is deliberately not executed here: with no
# filter it would list every function.) ---
$CLICKHOUSE_CLIENT --query "SHOW TABLES LIKE ''"

# --- A rule for the empty-pattern form must not match the query without the clause, and vice
# versa. The rule names are suffixed with the test database so the test stays parallel-safe
# despite the global rule registry; only whether the rule fired (rejection) is asserted. ---
RULE_EMPTY_LIKE="rule_tables_empty_like_${CLICKHOUSE_DATABASE}"
RULE_PLAIN="rule_tables_plain_${CLICKHOUSE_DATABASE}"
RULE_TRUNCATE="rule_truncate_empty_like_${CLICKHOUSE_DATABASE}"

check() { # $1 = label, $2 = output; prints whether the rule fired
    if echo "$2" | grep -q "REWRITE_RULE_REJECTION"; then echo "$1: rejected"; else echo "$1: not rejected"; fi
}

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_EMPTY_LIKE} AS (SHOW TABLES LIKE '') REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_EMPTY_LIKE}" --query "SHOW TABLES LIKE ''" 2>&1 || true)
check "tables empty-like exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_EMPTY_LIKE}" --query "SHOW TABLES" 2>&1 || true)
check "tables empty-like vs plain" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_EMPTY_LIKE}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_PLAIN} AS (SHOW TABLES) REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_PLAIN}" --query "SHOW TABLES" 2>&1 || true)
check "tables plain exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_PLAIN}" --query "SHOW TABLES LIKE ''" 2>&1 || true)
check "tables plain vs empty-like" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_PLAIN}"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE_TRUNCATE} AS (TRUNCATE TABLES FROM db LIKE '') REJECT WITH 'blocked'"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_TRUNCATE}" --query "TRUNCATE TABLES FROM db LIKE ''" 2>&1 || true)
check "truncate empty-like exact" "$out"
out=$($CLICKHOUSE_CLIENT --query_rules="${RULE_TRUNCATE}" --query "TRUNCATE TABLES FROM db" 2>&1 || true)
check "truncate empty-like vs plain" "$out"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE_TRUNCATE}"
