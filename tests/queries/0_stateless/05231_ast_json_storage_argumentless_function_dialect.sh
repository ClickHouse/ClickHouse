#!/usr/bin/env bash
# The `clickhouse_json` dialect is the entry point that builds storage metadata from a restored AST,
# which no `.sql` test can reach: those go through `formatQueryFromJSON`, which never runs the
# metadata builders. Without the boundary check the server does not fail these payloads, it dies on
# them (`Assertion 'px != 0' failed` in `checkFunctionIsInOrGlobalInOperator` for the `in` family,
# and in `ActionsMatcher::visit` for `arrayJoin`/`grouping`), so the closing liveness query is part
# of the assertion.
#
# The payloads go over HTTP rather than through `clickhouse client --dialect clickhouse_json`,
# because the client deserializes the JSON itself and would reject them before the server sees them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

JSON_URL="${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json"
ARGS_A_1=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'
ARGS_A=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}'
ARGS_X_1=',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"x"},{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}'
ARRAY_A=',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"is_operator":true}]}'
ARRAY_D=',"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"array","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"}]},"is_operator":true}]}'

# $1 = statement to serialize, $2 = the "arguments" member to drop
payload() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$2', '') FORMAT TSVRaw"
}

send() {
    ${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary "$1" | grep -om1 "has no 'arguments' list"
}

MT="ENGINE = MergeTree ORDER BY a"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tk"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tk2"

# The four storage key slots, analysed by `KeyDescription::getKeyFromAST`.
send "$(payload "CREATE TABLE tk (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8, b UInt8) ENGINE = MergeTree PRIMARY KEY a IN (1) ORDER BY b" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8) $MT SAMPLE BY a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8) $MT PARTITION BY a IN (1)" "$ARGS_A_1")"

# A skip index, analysed by `IndexDescription::initExpressionInfo`.
send "$(payload "CREATE TABLE tk (a UInt8, INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1) $MT" "$ARRAY_A")"

# The three TTL expression slots, analysed by `TTLDescription::getTTLFromAST`. The `GROUP BY ... SET`
# assignment has to be NESTED: unnested, an assignment-shape check answers before analysis.
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL grouping(a)" "$ARGS_A")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL d + toIntervalDay(1) DELETE WHERE a IN (1)" "$ARGS_A_1")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime TTL arrayJoin([d])) $MT" "$ARRAY_D")"
send "$(payload "CREATE TABLE tk (a UInt8, d DateTime) $MT TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d]))" "$ARRAY_D")"

# The `ALTER` and `CREATE INDEX` routes into the same slots need the table to exist first, or the
# payload stops at UNKNOWN_TABLE before the expression is analysed.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tk (a UInt8, b UInt8, d DateTime) $MT"

send "$(payload "ALTER TABLE tk (MODIFY ORDER BY a IN (1))" "$ARGS_A_1")"
send "$(payload "ALTER TABLE tk (MODIFY SAMPLE BY a IN (1))" "$ARGS_A_1")"
send "$(payload "ALTER TABLE tk (MODIFY TTL arrayJoin([d]))" "$ARRAY_D")"
send "$(payload "ALTER TABLE tk (MODIFY TTL d + toIntervalDay(1) GROUP BY a SET d = max(arrayJoin([d])))" "$ARRAY_D")"
send "$(payload "ALTER TABLE tk (ADD INDEX i arrayJoin([a]) TYPE minmax GRANULARITY 1)" "$ARRAY_A")"
send "$(payload "CREATE INDEX i ON tk (arrayJoin([a])) TYPE minmax GRANULARITY 1" "$ARRAY_A")"
send "$(payload "ALTER TABLE tk (MODIFY COLUMN d DateTime TTL arrayJoin([d]))" "$ARRAY_D")"

# `MODIFY TTL` also escapes through a substituted child TYPE: the ALTER side imposed no type on the
# `ttl` list, so an argument-less function can sit directly in it instead of inside an `ASTTTLElement`,
# and `TTLDescription::getTTLFromAST` then reads it as a column TTL. The payload replaces the whole
# element, so it is built by substitution rather than by dropping an "arguments" member.
TTL_ELEM='{"type":"TTLElement","mode":"DELETE","destination_type":"DELETE","if_exists":false,"ttl_expr":{"type":"Function","name":"plus","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"d"},{"type":"Function","name":"toIntervalDay","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"is_operator":true}}'
substituted() {
    ${CLICKHOUSE_CLIENT} --query "SELECT replace(parseQueryToJSON('$1'), '$TTL_ELEM', '$2') FORMAT TSVRaw"
}
send "$(substituted "ALTER TABLE tk (MODIFY TTL d + toIntervalDay(1))" '{"type":"Function","name":"in"}')"

# A foreign child that is NOT an argument-less function reaches the same column-TTL branch without the
# recursive screen ever seeing a reason to reject it, so the restored list-of-TTL-elements contract is
# the only thing that answers it. It is not a crash, so this row asserts the contract's message.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(substituted "ALTER TABLE tk (MODIFY TTL d + toIntervalDay(1))" '{"type":"Identifier","name":"d"}')" |
    grep -om1 "must be a list of TTL elements"

# `INSERT INTO FUNCTION ... PARTITION BY` reaches the same key builder through `StorageFile::write`,
# which only takes the partitioned branch when the path has a wildcard. The inner quotes are doubled
# because `payload` nests the statement inside a SQL string literal.
send "$(payload "INSERT INTO TABLE FUNCTION file(''05231_p_{_partition_id}.csv'', ''CSV'', ''a UInt8'') PARTITION BY a IN (1) SELECT 1" "$ARGS_A_1")"

# The deprecated positional `MergeTree(date, key, granularity)` arguments become key expressions. The
# setting gates that branch at CREATE, so without it the payload stops before the expression is used.
${CLICKHOUSE_CURL} -sS "${JSON_URL}&allow_deprecated_syntax_for_merge_tree=1" --data-binary \
    "$(payload "CREATE TABLE tk2 (d Date, x UInt64) ENGINE = MergeTree(d, x IN (1), 8192)" "$ARGS_X_1")" |
    grep -om1 "has no 'arguments' list"

# The `ttl` slot's own valid shapes still apply through the same entry point. `RECOMPRESS CODEC(...)`
# is the one that an over-broad screen would break: the codec is an argument-less-by-construction
# function list, held outside `IAST::children` so the recursive walk never reaches it.
for ttl in 'd + toIntervalDay(1)' 'd + toIntervalDay(1) RECOMPRESS CODEC(ZSTD(3))' \
           'd + toIntervalDay(1) GROUP BY a SET d = max(d)'; do
    ${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
        "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('ALTER TABLE tk (MODIFY TTL $ttl)') FORMAT TSVRaw")"
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE tk" | grep -oFm1 "TTL $ttl"
done

# `clickhouse-local` runs the dialect in the same process, with no server to protect it.
${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json \
    --query "$(payload "CREATE TABLE tk (a UInt8) ENGINE = MergeTree ORDER BY a IN (1)" "$ARGS_A_1")" 2>&1 |
    grep -om1 "has no 'arguments' list"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tk"

# A well-formed payload still applies every screened slot through the same entry point, including the
# `TYPE minmax` and column `CODEC` functions that are argument-less by construction.
${CLICKHOUSE_CURL} -sS "$JSON_URL" --data-binary \
    "$(${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON('CREATE TABLE tk (a UInt8, b UInt8 CODEC(LZ4), d DateTime TTL d + toIntervalDay(2), INDEX i a TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY a % 8 PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a TTL d + toIntervalDay(1) GROUP BY a SET d = max(d)') FORMAT TSVRaw")"
for pattern in 'PARTITION BY a % 8' 'PRIMARY KEY a' 'ORDER BY (a, b)' 'SAMPLE BY a' \
               'INDEX i a TYPE minmax GRANULARITY 1' 'CODEC(LZ4)' \
               'TTL d + toIntervalDay(2)' 'GROUP BY a SET d = max(d)'; do
    ${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE tk" | grep -oFm1 "$pattern"
done
${CLICKHOUSE_CLIENT} --query "DROP TABLE tk"

# The server survived every rejection.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
