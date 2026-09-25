#!/usr/bin/env bash
# Regression test for a SIGSEGV in projection analysis: a `ProjectionDeclaration`
# restored from JSON whose `query` subtree carried an `ASTFunction` without an
# `arguments` list left `ASTFunction::arguments` null, and the window-expression
# collector in `TreeRewriter::analyzeSelect` dereferenced it unconditionally.
# The `query` slot is now screened as an expression slot, so the malformed
# payload is rejected with BAD_ARGUMENTS instead of killing the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04952 SYNC"

# A valid CREATE TABLE ... PROJECTION round-trips through the JSON dialect.
JSON=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('CREATE TABLE t_04952 (x UInt8, PROJECTION p (SELECT count() GROUP BY x)) ENGINE = MergeTree ORDER BY x') FORMAT TSVRaw")
${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04952 SYNC"

# The same payload with the `arguments` member removed from the `count`
# Function node must now be rejected at the AST JSON boundary, not crash.
JSON_BAD=$(printf '%s' "$JSON" | python3 -c '
import json, sys

def strip_args(node):
    if isinstance(node, dict):
        if node.get("type") == "Function" and node.get("name") == "count":
            node.pop("arguments", None)
        for v in node.values():
            strip_args(v)
    elif isinstance(node, list):
        for v in node:
            strip_args(v)

ast = json.load(sys.stdin)
strip_args(ast)
print(json.dumps(ast))
')

OUT=$(${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_BAD" 2>&1 || true)
echo "$OUT" | grep -oE 'BAD_ARGUMENTS' | head -1
echo "$OUT" | grep -oE "has no 'arguments' list" | head -1

# The server is still alive to serve a plain query.
${CLICKHOUSE_CLIENT} -q 'SELECT 1'
