#!/usr/bin/env bash
# Malformed JSON AST: a function without an `arguments` list inside a projection expression slot
# (`query` for a SELECT projection, `index` for an INDEX projection) must be rejected with BAD_ARGUMENTS.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# $1: projection definition, $2: name of the function whose `arguments` are removed
function check()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04952 SYNC"

    # The valid statement round-trips through the JSON dialect.
    JSON=$(${CLICKHOUSE_CLIENT} -q "SELECT parseQueryToJSON('CREATE TABLE t_04952 (x UInt8, PROJECTION $1) ENGINE = MergeTree ORDER BY x') FORMAT TSVRaw")
    ${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04952 SYNC"

    JSON_BAD=$(printf '%s' "$JSON" | python3 -c '
import json, sys

def strip_args(node, name):
    if isinstance(node, dict):
        if node.get("type") == "Function" and node.get("name") == name:
            node.pop("arguments", None)
        for v in node.values():
            strip_args(v, name)
    elif isinstance(node, list):
        for v in node:
            strip_args(v, name)

ast = json.load(sys.stdin)
strip_args(ast, sys.argv[1])
print(json.dumps(ast))
' "$2")

    OUT=$(${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_BAD" 2>&1 || true)
    echo "$OUT" | grep -oE 'BAD_ARGUMENTS' | head -1
    echo "$OUT" | grep -oE "has no 'arguments' list" | head -1

    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_04952 SYNC"
}

check "p (SELECT count() GROUP BY x)" count
check "p INDEX x + 1 TYPE basic" plus

# The server is still alive to serve a plain query.
${CLICKHOUSE_CLIENT} -q 'SELECT 1'
