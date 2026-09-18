#!/usr/bin/env bash
# The `clickhouse_json` dialect builds storage metadata from a restored AST, which `formatQueryFromJSON`
# (05230) never reaches. Without the boundary screen the server does not fail these payloads, it dies on
# them (`Assertion 'px != 0' failed` in `checkFunctionIsInOrGlobalInOperator` for the `in` family, in
# `ActionsMatcher::visit` for `arrayJoin`/`grouping`), so the closing liveness query is part of the
# assertion. The payloads go over HTTP rather than through `clickhouse client --dialect clickhouse_json`,
# because the client deserializes the JSON itself and would reject them before the server sees them.
#
# One case per screened slot, and the reference records the slot key the screen reports, so a payload that
# failed to build or a rejection from the wrong slot prints the wrong line instead of passing silently.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"
ARGS_A_1=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'
ARGS_A=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}'
ARGS_X_1=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'
ARRAY_A=',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"is_operator":true}]}'
ARRAY_D=',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}'
MT="ENGINE = MergeTree ORDER BY a"

# $1 = statement to serialize, $2 = the "arguments" member to drop
payload() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$2', '') FORMAT TSVRaw"
}

send() {
    ${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary "$1" |
        grep -oEm1 "for key '[a-z_]+' has no 'arguments' list"
}

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tk"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tk2"

# The four `ASTStorage` key slots, analysed by `KeyDescription::getKeyFromAST`, and one nested inside a
# larger key expression, which only the recursive walk catches.
send "$(payload "CREATE TABLE tk (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a IN (1) ORDER BY b" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8) $MT SAMPLE BY a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8) $MT PARTITION BY a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8) ENGINE = MergeTree ORDER BY (a, a IN (1))" "$ARGS_A_1")"

# A skip index and a column TTL, analysed by `IndexDescription::initExpressionInfo` and
# `TTLDescription::getTTLFromAST`. The `ALTER`/`CREATE INDEX` routes into both read the same declaration
# node, so they are covered by these two.
send "$(payload "CREATE TABLE tk (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) $MT" "$ARRAY_A")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime TTL arrayJoin([d])) $MT" "$ARRAY_D")"

# Both `ASTTTLElement` expression slots, and the `ASTAssignment` of a `GROUP BY ... SET`, which has to be
# NESTED: unnested, an assignment-shape check answers before analysis.
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL grouping(a)" "$ARGS_A")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL d + toIntervalDay(1) DELETE WHERE a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d]))" "$ARRAY_D")"

# The `ALTER` command's own slots need the table to exist, or the payload stops at UNKNOWN_TABLE before
# the expression is analysed.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tk (a UInt8, b UInt8, d DateTime) $MT"
send "$(payload "ALTER TABLE tk (MODIFY ORDER BY a IN (1))" "$ARGS_A_1")"
send "$(payload "ALTER TABLE tk (MODIFY SAMPLE BY a IN (1))" "$ARGS_A_1")"

# `MODIFY TTL` also escapes through a substituted child type: the ALTER side imposed no type on the `ttl`
# list, so a function can sit directly in it instead of inside an `ASTTTLElement`, and
# `TTLDescription::getTTLFromAST` then reads it as a column TTL. An argument-less function there is the
# crash; an identifier reaches the same branch and only the restored list-of-TTL-elements contract answers
# it, so that row asserts the contract's own message.
TTL_ELEM='{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}'
substituted() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$TTL_ELEM', '$2') FORMAT TSVRaw"
}
send "$(substituted "ALTER TABLE tk (MODIFY TTL d + toIntervalDay(1))" '{"type":"Function","name":"in"}')"
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(substituted "ALTER TABLE tk (MODIFY TTL d + toIntervalDay(1))" '{"type":"Identifier","name":"d"}')" |
    grep -om1 "must be a list of TTL elements"

# `INSERT INTO FUNCTION ... PARTITION BY` reaches the same key builder through `StorageFile::write`, which
# only takes the partitioned branch when the path has a wildcard. The inner quotes are doubled because
# `payload` nests the statement inside a SQL string literal.
send "$(payload "INSERT INTO TABLE FUNCTION file(''05231_p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a IN (1) SELECT 1" "$ARGS_A_1")"

# The deprecated positional `MergeTree(date, key, granularity)` arguments become key expressions. The
# setting gates that branch at CREATE, so without it the payload stops before the expression is used.
${CLICKHOUSE_CURL} -sS "${JSON_URL}&allow_deprecated_syntax_for_merge_tree=1" --data-binary \
    "$(payload "CREATE TABLE tk2 (d Date, x UInt64) ENGINE = MergeTree(d, x IN (1), 8192)" "$ARGS_X_1")" |
    grep -oEm1 "for key '[a-z_]+' has no 'arguments' list"

# `clickhouse-local` runs the dialect in the same process, with no server to protect it.
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json \
    --query "$(payload "CREATE TABLE tk (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)" "$ARGS_A_1")" 2>&1 |
    grep -oEm1 "for key '[a-z_]+' has no 'arguments' list"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tk"

# A well-formed payload still applies every screened slot through the same entry point, including the
# `TYPE minmax`, `CODEC` and `RECOMPRESS CODEC` functions that are argument-less by construction and that
# an over-broad screen would reject.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('CREATE TABLE tk (a UInt8, b UInt8 CODEC(LZ4), d DateTime TTL d + toIntervalDay(2), INDEX i a TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY a % 8 PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a TTL d + toIntervalDay(1) RECOMPRESS CODEC(LZ4)') FORMAT TSVRaw")"
for pattern in 'PARTITION BY a % 8' 'ORDER BY (a, b)' 'SAMPLE BY a' 'INDEX i a TYPE minmax GRANULARITY 1' \
               'UInt8 CODEC(LZ4)' 'TTL d + toIntervalDay(1) RECOMPRESS CODEC(LZ4)'; do
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE tk" | grep -oFm1 "$pattern"
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE tk"

# The server survived every rejection.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
