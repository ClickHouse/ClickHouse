#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: rewrite rules are global server state

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An INSERT query carrying inline data (VALUES / FORMAT) must not be storable as a rule
# template: `ASTInsertQuery::clone` keeps the raw `data` / `end` pointers, which reference the
# original CREATE RULE query buffer (freed before the stored rule is used). The parser already
# rejects such a template (the inline data swallows the closing parenthesis), and `RewriteRules`
# also rejects it defensively, so the memory-safety guarantee does not rely on parser behaviour.
# Each statement is sent separately because inline VALUES data consumes the rest of its buffer.
# An `INSERT ... SELECT` (no inline data) is allowed.

# Rule names are global; make them unique per test database. `DROP RULE` has no `IF EXISTS`.
RULE="rule_insert_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}_sel" 2>/dev/null

echo "inline VALUES in source template:"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE}_v AS (INSERT INTO ${RULE}_t VALUES (1)) REWRITE TO (SELECT 1)" 2>&1 | grep -o -m1 "SYNTAX_ERROR"
echo "inline VALUES in result template:"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE}_r AS (SELECT 1) REWRITE TO (INSERT INTO ${RULE}_t VALUES (1))" 2>&1 | grep -o -m1 "SYNTAX_ERROR"

echo "INSERT ... SELECT (no inline data) is allowed:"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE}_sel AS (INSERT INTO ${RULE}_t SELECT 1) REWRITE TO (SELECT 1)"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}_sel'"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}_sel"
