#!/usr/bin/env bash
# Tags: no-old-analyzer
# ^ `QueryNormalizer` treats `max_ast_depth = 0` as a literal limit of zero instead of "no limit",
#   so with the old analyzer every query below fails with `TOO_DEEP_AST` before it runs.

# `max_ast_depth = 0` disables the semantic AST depth check, but JSON AST deserialization
# recurses over the JSON document, so the stack-safety ceiling must still apply: an overly
# deep payload has to fail with a controlled exception on every entry point.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A JSON text whose bracket nesting alone exceeds the ceiling. The pre-scan rejects it before
# `Poco::JSON::Parser` ever recurses over it, so it does not have to be a well-formed document.
BRACKET_BOMB="{\"type\":\"Literal\",\"value\":$(printf '[%.0s' $(seq 1 9000))"

# 1. `formatQueryFromJSON` with a deeply nested chain of real AST nodes.
${CLICKHOUSE_CLIENT} --max_ast_depth 0 --query "
    SELECT formatQueryFromJSON(concat(
        repeat('{\"type\":\"ExpressionList\",\"children\":[', 4500),
        '{\"type\":\"Literal\",\"value\":{\"field_type\":\"UInt64\",\"value\":1}}',
        repeat(']}', 4500)))" 2>&1 | grep -om1 'TOO_DEEP_AST'

# 2. `formatQueryFromJSON` with the raw bracket bomb.
${CLICKHOUSE_CLIENT} --max_ast_depth 0 --param_json "$BRACKET_BOMB" \
    --query "SELECT formatQueryFromJSON({json:String})" 2>&1 | grep -om1 'TOO_DEEP_AST'

# 3. A deeply nested structured `Field` value adds no AST nodes and stays under the bracket
#    budget, so only the `Field` depth bound rejects it.
${CLICKHOUSE_CLIENT} --max_ast_depth 0 --query "
    SELECT formatQueryFromJSON(concat(
        '{\"type\":\"Literal\",\"value\":',
        repeat('{\"field_type\":\"Array\",\"value\":[', 2000),
        '{\"field_type\":\"UInt64\",\"value\":1}',
        repeat(']}', 2000), '}'))" 2>&1 |
    grep -om1 'Structured Field value exceeds maximum AST depth limit'

# 4. A deeply nested `Field` dump hides its nesting inside a JSON string, so the bracket
#    pre-scan does not see it either; `Field::restoreFromDump` must not recurse unbounded.
${CLICKHOUSE_CLIENT} --max_ast_depth 0 --query "
    SELECT formatQueryFromJSON(concat(
        '{\"type\":\"Literal\",\"value\":{\"field_type\":\"Object\",\"value\":\"',
        repeat('Array_[', 2000), repeat(']', 2000), '\"}}'))" 2>&1 |
    grep -om1 'Field dump payload exceeds maximum AST depth limit'

# 5. Sanity: with `max_ast_depth = 0` a normal query still round-trips byte-identically.
${CLICKHOUSE_CLIENT} --max_ast_depth 0 --query "
    SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2 AS x FROM numbers(10) WHERE x > 0'))
        = formatQuerySingleLine('SELECT 1 + 2 AS x FROM numbers(10) WHERE x > 0')"

# 6. The server-side `clickhouse_json` dialect entry point.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&max_ast_depth=0" \
    --data-binary "$BRACKET_BOMB" 2>&1 | grep -om1 'TOO_DEEP_AST'

# 7. The `clickhouse-local` `clickhouse_json` dialect entry point.
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json --max_ast_depth 0 \
    --query "$BRACKET_BOMB" 2>&1 | grep -om1 'TOO_DEEP_AST'
