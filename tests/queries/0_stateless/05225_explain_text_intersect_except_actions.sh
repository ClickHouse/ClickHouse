#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `MODIFY LIMIT`, `MODIFY OFFSET` and `PAGE` accept only a single plain `SELECT`.
# `getSingleSelectQuery` relies on `as<ASTSelectQuery>` being an exact-type cast, so an
# `ASTSelectIntersectExceptQuery` (a subclass whose children are set-operation operands,
# not clauses) must be refused on every path that can carry one.

# SQL: the parser keeps `EXCEPT` / `INTERSECT` as a flat multi-branch union.
for action in 'MODIFY LIMIT 5' 'MODIFY OFFSET 5' 'PAGE 2'; do
    echo "SQL EXCEPT $action: $(${CLICKHOUSE_CLIENT} --query "EXPLAIN TEXT (SELECT 1 EXCEPT ALL SELECT 2) $action" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1)"
    echo "SQL INTERSECT $action: $(${CLICKHOUSE_CLIENT} --query "EXPLAIN TEXT (SELECT 1 LIMIT 3 INTERSECT SELECT 2) $action" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1)"
done

# JSON: the only way to hand the rewrite a real `SelectIntersectExceptQuery` node, either as
# the direct source or as the single branch of a `SelectWithUnionQuery`.
literal_one='{"type":"Literal","value":{"field_type":"UInt64","value":1}}'
select_with_limit='{"type":"SelectQuery","select":{"type":"ExpressionList","children":['"$literal_one"']},"limit_length":{"type":"Literal","value":{"field_type":"UInt64","value":3}}}'
select_plain='{"type":"SelectQuery","select":{"type":"ExpressionList","children":['"$literal_one"']}}'
except_query='{"type":"SelectIntersectExceptQuery","final_operator":"EXCEPT ALL","children":['"$select_with_limit"','"$select_plain"']}'
wrapped_except_query='{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":['"$except_query"']}}'

for kind in 'MODIFY LIMIT' 'MODIFY OFFSET' 'PAGE'; do
    operand_value=5
    [ "$kind" = PAGE ] && operand_value=2
    action='{"type":"ExplainTextAction","kind":"'"$kind"'","operand":{"type":"Literal","value":{"field_type":"UInt64","value":'"$operand_value"'}}}'
    for source_name in except_query wrapped_except_query; do
        source=${!source_name}
        result=$(${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&default_format=TabSeparated" \
            --data-binary '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":'"$source"',"actions":{"type":"ExpressionList","children":['"$action"']}}' 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1)
        echo "JSON $source_name $kind: $result"
    done
done

# `MODIFY FORMAT` is the one action a set operation supports, and only through the union wrapper
# that carries the output options; the bare node is refused at deserialization like it is for SQL.
result=$(${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&default_format=TabSeparated" \
    --data-binary '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":'"$except_query"',"actions":{"type":"ExpressionList","children":[{"type":"ExplainTextAction","kind":"MODIFY FORMAT","operand":{"type":"Identifier","name":"CSV"}}]}}' 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1)
echo "JSON except_query MODIFY FORMAT: $result"
echo "JSON wrapped_except_query MODIFY FORMAT:"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json&default_format=TabSeparated" \
    --data-binary '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":'"$wrapped_except_query"',"actions":{"type":"ExpressionList","children":[{"type":"ExplainTextAction","kind":"MODIFY FORMAT","operand":{"type":"Identifier","name":"CSV"}},{"type":"ExplainTextAction","kind":"ONELINE"}]}}'
