#!/usr/bin/env bash
# `EXPLAIN AST optimize = 1` is the one route that still reaches the pre-analyzer select interpreter:
# `InterpreterFactory` turns the analyzer off for `ParsedAST`, and the legacy visitors dereference
# `ASTFunction::arguments` with no arity check in front of them. Without the boundary screen the server
# does not fail these payloads, it dies on them (`Assertion 'px != 0' failed`), so the closing liveness
# query is part of the assertion. The payloads go over HTTP rather than through
# `clickhouse client --dialect clickhouse_json`, because the client deserializes the JSON itself and
# would reject them before the server sees them.
#
# One case per screened slot, and the reference records the slot key the screen reports, so a payload
# that failed to build or a rejection from the wrong slot prints the wrong line instead of passing
# silently. Each deleted `arguments` member is picked out by a distinct literal so that one `replace`
# malforms exactly one node.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"

ARGS_1_2=',"arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}'
ARGS_X_3=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":3}}]}'
ARGS_A_5=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":5}}]}'
ARGS_1_6=',"arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":6}}]}'
ARGS_A_7=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":7}}]}'
ARGS_A_8=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":8}}]}'
ARGS_X_9=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":9}}]}'

# $1 = statement to serialize, $2 = the "arguments" member to drop
payload() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$2', '') FORMAT TSVRaw"
}

send() {
    ${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary "$1" |
        grep -oEm1 "for key '[a-z_]+' has no 'arguments' list"
}

# The two generic slot readers of `ASTSelectQuery::readJSON`: a scalar expression slot (`where`) and a
# list slot (`group_by`). `count()` in the same payload is argument-less in the SQL but carries an empty
# `arguments` list, which is what the parser produces and the screen accepts.
send "$(payload "EXPLAIN AST optimize = 1 SELECT 1 WHERE 1 IN (2)" "$ARGS_1_2")"
send "$(payload "EXPLAIN AST optimize = 1 SELECT count() FROM (SELECT 1 AS a) GROUP BY a + 5" "$ARGS_A_5")"

# The `SelectWithUnionQuery` wrapper the parser always builds is not load-bearing: replacing the `query`
# slot with its own bare `SelectQuery` child reaches the same visitor, so a screen placed on the EXPLAIN
# slot and keyed on the wrapper type would be bypassed. Keeping the invariant on the SELECT node makes
# the wrapper shape irrelevant.
bare_payload() {
    ${CLICKHOUSE_CLIENT} --query "WITH replace(parseQueryToJSON('$1'), '$2', '') AS p
        SELECT replace(p, JSONExtractRaw(p, 'query'), JSONExtractRaw(p, 'query', 'list_of_selects', 'children', 1)) FORMAT TSVRaw"
}
send "$(bare_payload "EXPLAIN AST optimize = 1 SELECT 1 WHERE 1 IN (2)" "$ARGS_1_2")"

# `tables` and `interpolate` are read with their own typed reads rather than through those two lambdas.
send "$(payload "EXPLAIN AST optimize = 1 SELECT a FROM (SELECT 1 AS a) ARRAY JOIN [a + 7] AS j" "$ARGS_A_7")"
send "$(payload "EXPLAIN AST optimize = 1 SELECT a FROM (SELECT 1 AS a) ORDER BY a WITH FILL FROM 1 TO 2 INTERPOLATE (a AS a + 8)" "$ARGS_A_8")"

# `ASTColumnsApplyTransformer` keeps both of its AST members out of `IAST::children`, so the recursive
# walk over the SELECT cannot reach them and each needs the screen at its own slot. `parameters` is
# parsed with `ParserExpressionList` and holds arbitrary expressions, not only literals.
send "$(payload "EXPLAIN AST optimize = 1 SELECT * APPLY(x -> x + 3) FROM (SELECT 1 AS a)" "$ARGS_X_3")"
send "$(payload "EXPLAIN AST optimize = 1 SELECT * APPLY(quantile(1 IN (6))) FROM (SELECT 1 AS a)" "$ARGS_1_6")"

# A SQL UDF body is not written into the SELECT it breaks: the DDL persists the node, and
# `UserDefinedSQLFunctionVisitor` splices it into a caller during `TreeRewriter::normalize`, after
# deserialization has finished. The name carries the test database because a SQL UDF is server-wide.
UDF="${CLICKHOUSE_DATABASE}_udf_argumentless"
${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${UDF}"
send "$(payload "CREATE FUNCTION ${UDF} AS (x) -> x IN (9)" "$ARGS_X_9")"
# Rejected at the boundary means nothing was persisted, so no caller can reach the visitor through it.
${CLICKHOUSE_CLIENT} --query "EXPLAIN AST optimize = 1 SELECT ${UDF}(1)" 2>&1 | grep -oEm1 'UNKNOWN_FUNCTION'
# The same statement well-formed is stored, and the body it restores still reaches that path: the two
# lines below are the `in` node expanded into the caller, with the body's own literal under it.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('CREATE FUNCTION ${UDF} AS (x) -> x IN (9)') FORMAT TSVRaw")"
${CLICKHOUSE_CLIENT} --query "EXPLAIN AST optimize = 1 SELECT ${UDF}(1)" |
    grep -oE "Function in \(children 1\)|Literal UInt64_9"
${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${UDF}"

# Well-formed payloads still round-trip through every screened slot, including the nullary `count()`,
# the `numbers` table function and both transformer members, whose `arguments` lists the parser leaves
# empty. `EXPLAIN SYNTAX` reaches the same visitor and prints the formatted SQL, so the reference
# asserts what was restored rather than only the absence of an error.
explain_syntax() {
    ${CLICKHOUSE_CURL} -sS "${JSON_URL}&default_format=TSVRaw" --data-binary \
        "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('$1') FORMAT TSVRaw")"
}
explain_syntax "EXPLAIN SYNTAX SELECT count() FROM numbers(3) GROUP BY number ORDER BY number WITH FILL INTERPOLATE (number AS number + 1)"
explain_syntax "EXPLAIN SYNTAX SELECT * APPLY(x -> x + 1) APPLY(quantile(0.5)) FROM (SELECT 1 AS a)"

# The server survived every rejection.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
