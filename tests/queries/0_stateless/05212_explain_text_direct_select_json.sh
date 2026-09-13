#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

query_json='{
    "type": "ExplainQuery",
    "kind": "EXPLAIN TEXT",
    "query": {
        "type": "SelectQuery",
        "select": {
            "type": "ExpressionList",
            "children": [
                {"type": "Literal", "value": 1}
            ]
        }
    },
    "actions": {
        "type": "ExpressionList",
        "children": [
            {
                "type": "ExplainTextAction",
                "kind": "MODIFY FORMAT",
                "operand": {"type": "Identifier", "name": "CSV"}
            },
            {
                "type": "ExplainTextAction",
                "kind": "ONELINE"
            }
        ]
    }
}'

# JSON formatting agrees with the equivalent SQL statement.
${CLICKHOUSE_CLIENT} --query "
    SELECT formatQueryFromJSON('$query_json')
        = formatQuerySingleLine(
            'EXPLAIN TEXT SELECT 1 MODIFY FORMAT CSV, ONELINE')
"

# Execute the original JSON directly, preserving its bare `SelectQuery`.
${CLICKHOUSE_CURL} --fail --show-error \
    "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&default_format=TabSeparated" \
    --data-binary "$query_json"
