#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `max_ast_depth` / `max_ast_elements` must bound JSON AST deserialization while the tree is built,
# not only when the finished tree is re-checked. Both report the same error code, so match the message.
# The leaf carries no `Field`: reading one reports the same element-limit message from its own counter.

OPEN=$(printf '{"type":"ExpressionList","children":[%.0s' $(seq 1 30))
CLOSE=$(printf ']}%.0s' $(seq 1 30))
JSON="${OPEN}{\"type\":\"Asterisk\"}${CLOSE}"

${CLICKHOUSE_CLIENT} --max_ast_depth 10 --max_ast_elements 0 --param_json "$JSON" \
    --query "SELECT formatQueryFromJSON({json:String})" 2>&1 |
    grep -om1 'JSON AST deserialization exceeded maximum depth limit (10)'

${CLICKHOUSE_CLIENT} --max_ast_depth 1000 --max_ast_elements 12 --param_json "$JSON" \
    --query "SELECT formatQueryFromJSON({json:String})" 2>&1 |
    grep -om1 'JSON AST deserialization exceeded maximum element count limit (12)'

# A payload inside both bounds still deserializes.
${CLICKHOUSE_CLIENT} --max_ast_depth 10 --max_ast_elements 12 --query \
    "SELECT formatQueryFromJSON('{\"type\":\"ExpressionList\",\"children\":[{\"type\":\"Literal\",\"value\":{\"field_type\":\"UInt64\",\"value\":1}}]}')"
