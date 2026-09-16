#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lambda restored from `clickhouse_json` must be rejected before query analysis reads the children
# it declares. Each shape below took the whole process down, so they run through `clickhouse-local`
# rather than `${CLICKHOUSE_CLIENT}`, whose server is shared with the rest of the suite, and the
# oracle asserts both that the expected error is reported and that the process was not signalled.

run_json() { # $1 = expected error name, $2 = JSON AST
    local out rc
    out=$(${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$2" 2>&1)
    rc=$?
    if [ "$rc" -ge 128 ]; then
        echo "KILLED_BY_SIGNAL rc=$rc"
    elif echo "$out" | grep -qF "$1"; then
        echo "rejected: $1"
    else
        echo "UNEXPECTED rc=$rc: $(echo "$out" | head -1)"
    fi
}

# 1. Control: a well-formed lambda still executes through the dialect.
CONTROL=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('SELECT arrayMap(x -> x + 1, [1, 2, 3])') FORMAT TSVRaw")
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$CONTROL"

# 2. `is_lambda_function` on a function with no `arguments` at all. `QueryTreeBuilder::buildExpression`
#    takes the flag as proof of the `lambda(tuple(...), body)` shape and read the absent list.
run_json BAD_ARGUMENTS '{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Function","name":"lambda","is_lambda_function":true}]}}]}}'

# 3. A skip index expression that is a `lambda` function with no `arguments`. No flag is involved
#    here: `IndexDescription::initExpressionInfo` collects the declared argument names by function
#    name alone, for any index expression.
INDEX_JSON=$(${CLICKHOUSE_LOCAL} -q "SELECT replace(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX idx lambda TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY a'), '\"type\":\"Identifier\",\"name\":\"lambda\"', '\"type\":\"Function\",\"name\":\"lambda\"') FORMAT TSVRaw")
run_json NUMBER_OF_ARGUMENTS_DOESNT_MATCH "$INDEX_JSON"

# 4. The same collection reads the argument tuple's own argument list, which a `tuple` node restored
#    without one does not have. Written in function-call syntax, so the node carries no
#    `is_lambda_function` and the boundary check on that flag never inspects it.
INDEX_JSON_TUPLE=$(${CLICKHOUSE_LOCAL} -q "SELECT replace(parseQueryToJSON('CREATE TABLE t (a UInt8, INDEX idx lambda(tuple(a), a) TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY a'), '\"name\":\"tuple\",\"arguments\":{\"type\":\"ExpressionList\",\"children\":[{\"type\":\"Identifier\",\"name\":\"a\"}]}', '\"name\":\"tuple\"') FORMAT TSVRaw")
run_json TYPE_MISMATCH "$INDEX_JSON_TUPLE"

# 5. `CREATE FUNCTION` restores its core through the untyped `readChild`, and the validation that
#    registration performs on it tests nothing about the node beyond `as<ASTFunction>()`, so neither
#    the `is_lambda_function` boundary check nor a name test guards the argument list it then reads.
run_json BAD_ARGUMENTS '{"type":"CreateSQLFunctionQuery","function_name":{"type":"Identifier","name":"udf_shape"},"function_core":{"type":"Function","name":"lambda"}}'
