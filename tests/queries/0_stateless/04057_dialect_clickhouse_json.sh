#!/usr/bin/env bash
# Tags: no-fasttest

# Test the experimental gate `allow_experimental_json_ast_dialect` and the
# `dialect = 'clickhouse_json'` value, which lets queries be submitted as JSON
# ASTs (the output of `parseQueryToJSON`) instead of SQL text.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ============================================================================
# Defaults
# ============================================================================
echo "=== Default values ==="
$CLICKHOUSE_CLIENT --query "SELECT name, value, default FROM system.settings WHERE name IN ('allow_experimental_json_ast_dialect','dialect') ORDER BY name FORMAT TSV"

# ============================================================================
# Experimental gate
# ============================================================================
# The gate is checked at query-parse time, not at SET time. Setting the
# dialect itself succeeds without the gate; the failure happens when the
# next query is parsed.
echo "=== Without gate, client-side TCP query fails with SUPPORT_IS_DISABLED ==="
$CLICKHOUSE_CLIENT --dialect=clickhouse_json --query="SELECT 1" 2>&1 | grep -oE "Code: [0-9]+|SUPPORT_IS_DISABLED" | head -2

echo "=== Without gate, server-side HTTP query fails with SUPPORT_IS_DISABLED ==="
${CLICKHOUSE_CURL} -X POST "${CLICKHOUSE_URL}&dialect=clickhouse_json" --data-binary 'SELECT 1' 2>&1 | grep -oE "Code: [0-9]+|SUPPORT_IS_DISABLED" | head -2

# Error message mentions the setting to enable
echo "=== Error message mentions the setting to enable ==="
$CLICKHOUSE_CLIENT --dialect=clickhouse_json --query="SELECT 1" 2>&1 | grep -oE "allow_experimental_json_ast_dialect" | head -1

# ============================================================================
# Basic JSON-AST execution
# ============================================================================
echo "=== With gate, hand-written JSON-AST for SELECT 1 ==="
JSON_SELECT_1='{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="$JSON_SELECT_1"

# ============================================================================
# Round-trip: parseQueryToJSON('SQL') -> submit as JSON dialect -> same result
# ============================================================================
echo "=== Round-trip: SELECT 1 + 2 + 3 (arithmetic) ==="
JSON=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('SELECT 1 + 2 + 3')")
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="$JSON"

echo "=== Round-trip: SELECT with aliases ==="
JSON=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('SELECT 100 AS x, x * 2 AS y')")
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="$JSON"

echo "=== Round-trip: SELECT FROM numbers with WHERE / ORDER BY ==="
JSON=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('SELECT number FROM numbers(5) WHERE number > 1 ORDER BY number')")
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="$JSON"

echo "=== Round-trip: SELECT with GROUP BY and aggregates ==="
JSON=$($CLICKHOUSE_CLIENT --query "SELECT parseQueryToJSON('SELECT number % 3 AS k, count() AS c FROM numbers(10) GROUP BY k ORDER BY k')")
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="$JSON"

# ============================================================================
# Server-side SET-in-plain-SQL escape hatch
# ============================================================================
# When `dialect = 'clickhouse_json'`, a query that starts with `SET` is still
# parsed as plain SQL (server-side `executeQuery`), so the dialect can be
# switched back without the client having to know JSON-AST for SET.
echo "=== With gate, SET in plain SQL works under clickhouse_json dialect (HTTP) ==="
${CLICKHOUSE_CURL} -X POST "${CLICKHOUSE_URL}&allow_experimental_json_ast_dialect=1&dialect=clickhouse_json" --data-binary "SET dialect = 'clickhouse'"
echo "OK"

# ============================================================================
# Errors
# ============================================================================
echo "=== With gate, malformed JSON yields BAD_ARGUMENTS ==="
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="not json" 2>&1 | grep -oE "Code: [0-9]+|BAD_ARGUMENTS" | head -2

echo "=== With gate, empty JSON object yields BAD_ARGUMENTS ==="
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query="{}" 2>&1 | grep -oE "Code: [0-9]+|BAD_ARGUMENTS" | head -2

echo "=== With gate, unknown AST type yields BAD_ARGUMENTS ==="
$CLICKHOUSE_CLIENT --allow_experimental_json_ast_dialect=1 --dialect=clickhouse_json --query='{"type":"NoSuchASTNode"}' 2>&1 | grep -oE "Code: [0-9]+|BAD_ARGUMENTS" | head -2
