#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the DDL worker re-parses the DDL with the session `dialect`
# The `clickhouse_json` dialect runs a restored constraint declaration as DDL, which `formatQueryFromJSON`
# (05226) never reaches. Without the boundary check the server does not fail these payloads, it dies on
# them, so the closing liveness query is part of the assertion. The payloads go over HTTP rather than
# through `clickhouse client --dialect clickhouse_json`, because the client deserializes the JSON itself and
# would reject them before the server ever sees them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"
ARGS_A_0=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}'
ARGS_A=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}'
ARGS_X_0=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}'

# $1 = query to serialize, $2 = the "arguments" member to drop from the constraint expression
payload() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$2', '') FORMAT TSVRaw"
}

send() {
    ${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary "$1" | grep -om1 "has no 'arguments' list"
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tc"

# One row per crash consumer: a comparison, whose argument list `ComparisonGraph::getArguments`
# dereferences, and a logical function, whose list `TreeCNFConverter::splitMultiLogic` dereferences. The
# `ALTER ... ADD CONSTRAINT` route reads the same declaration node, and 05226 covers it at the boundary.
send "$(payload "CREATE TABLE tc (a UInt8, CONSTRAINT cc CHECK a > 0) ENGINE = MergeTree ORDER BY a" "$ARGS_A_0")"
send "$(payload "CREATE TABLE tc (a UInt8, CONSTRAINT cc CHECK NOT a) ENGINE = MergeTree ORDER BY a" "$ARGS_A")"

# `clickhouse-local` runs the dialect in the same process, with no server to protect it.
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json \
    --query "$(payload "CREATE TABLE tc (a UInt8, CONSTRAINT cc CHECK a > 0) ENGINE = MergeTree ORDER BY a" "$ARGS_A_0")" 2>&1 |
    grep -om1 "has no 'arguments' list"

# A well-formed payload still creates the table through the same entry point.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('CREATE TABLE tc (a UInt8, CONSTRAINT cc CHECK a > 0) ENGINE = MergeTree ORDER BY a') FORMAT TSVRaw")"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE tc" | grep -om1 'CONSTRAINT cc CHECK a > 0'
${CLICKHOUSE_CLIENT} --query "DROP TABLE tc"

# The rejection walks `IAST::children`, and `ASTColumnsApplyTransformer` owns its `lambda` outside that
# vector, so a function nested there is not screened - harmlessly, because the consumers walk `children`
# too, and the matcher is never expanded in a constraint.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(payload "CREATE TABLE tc (a UInt8, CONSTRAINT cc CHECK COLUMNS(''a'') APPLY (x -> x > 0)) ENGINE = MergeTree ORDER BY a" "$ARGS_X_0")"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE tc" | grep -om1 'x -> greater()'
${CLICKHOUSE_CLIENT} --query "DROP TABLE tc"

# The server survived every rejection.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
