#!/usr/bin/env bash
# Tags: no-replicated-database

# `viewIfPermitted` picks between its SELECT query and the `ELSE` table function according to the
# current user. A table created by `CREATE TABLE ... AS viewIfPermitted(...)` is resolved under the
# global context, which has no user and therefore full access, so the branch was decided as if the
# creator could read the source: a structure mismatch reported the source table's column names and
# types to a user that cannot even `DESCRIBE` it. Creating such a table is now refused; using the
# function directly in a query, where a user does exist, is unchanged.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_05060_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

# The names and types of ${db}.src_05060, none of which the failing statement is allowed to reveal.
# The same expression is used for the control below, so both read the same identifiers.
leaked() { grep -oE 'secret_a|secret_b|salary|ssn|Decimal|FixedString' | LC_ALL=C sort -u | tr '\n' ' ' | sed 's/ $//'; }

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "CREATE USER $user NOT IDENTIFIED"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE TABLE ON ${db}.* TO $user"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.src_05060 (secret_a UInt64, secret_b String, salary Decimal(18, 2), ssn FixedString(11)) ENGINE = MergeTree ORDER BY secret_a"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${db}.src_05060 SELECT number, 'b', 1, 'c' FROM numbers(3)"

# $user has no grant of any kind on ${db}.src_05060.

# The disclosure happens when the table is read, not when it is created: the table function is
# resolved lazily, so both statements below have to be attempted for the grep to be able to see it.
echo "--- CREATE TABLE AS viewIfPermitted is refused, and nothing is left to read ---"
out=$({ ${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${db}.dst1_05060 AS viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))"
        ${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM ${db}.dst1_05060"; } 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -o "UNKNOWN_TABLE"
echo "leaked: [$(echo "$out" | leaked)]"

echo "--- refused with an explicit column list too ---"
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${db}.dst2_05060 (x UInt64) AS viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))" 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -o "BAD_ARGUMENTS"

# The refusal is unconditional, so even a full-access user cannot persist the function nested in
# another table function: `remote(...)` would otherwise store it and later decide the guarded branch
# under the connection's credentials instead of the reader's grants.
echo "--- nested in remote(...) is refused too, even for a full-access user ---"
out=$({ ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.dst3_05060 AS remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64')))"
        ${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM ${db}.dst3_05060"; } 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -o "UNKNOWN_TABLE"
echo "leaked: [$(echo "$out" | leaked)]"

echo "--- nested through loop(...) inside remote(...) is refused too ---"
out=$(${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${db}.dst4_05060 AS remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', loop(viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))))" 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -o "BAD_ARGUMENTS"

# Without a column list the structure is inferred from the table function before the table is
# created, and that inference has side effects of its own: `remote(...)` connects to the shards and an
# `ELSE` arm is analyzed. The refusal has to come before any of that, so an unreachable shard must not
# change the outcome: it is `BAD_ARGUMENTS`, not a connection error, and no source identifier shows up.
echo "--- refused before the structure is inferred: an unreachable remote(...) shard is never contacted ---"
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${db}.dst7_05060 AS remote('127.0.0.1:1', viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64')))" 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -oE "BAD_ARGUMENTS|NO_REMOTE_SHARD_AVAILABLE|ALL_CONNECTION_TRIES_FAILED"
echo "leaked: [$(echo "$out" | leaked)]"

echo "--- and an unreachable remote(...) in the ELSE arm is not analyzed either ---"
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${db}.dst8_05060 AS viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE remote('127.0.0.1:1', system.one))" 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -oE "BAD_ARGUMENTS|NO_REMOTE_SHARD_AVAILABLE|ALL_CONNECTION_TRIES_FAILED"
echo "leaked: [$(echo "$out" | leaked)]"

# The `AS <table function>` path is not the only carrier of a table function into a persisted
# definition: the `Remote` / `RemoteSecure` engines store one in `remote_table_function_ptr`. The SQL
# parser only produces `viewIfPermitted(SELECT ... ELSE ...)` in table-expression position, but the
# `clickhouse_json` dialect executes a deserialized AST, so the shape can be carried in engine
# arguments. Splice the two ASTs together instead of writing the JSON out by hand, so the test
# follows the serialization format rather than pinning it.
json_create_query() {
    ${CLICKHOUSE_CLIENT} --enable_json_ast_dialect 1 --dialect clickhouse_json --query "$1"
}

engine_ast() {
    ${CLICKHOUSE_CLIENT} --query "SELECT parseQueryToJSON(\$\$CREATE TABLE ${db}.$1 ENGINE = Remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', numbers(10))\$\$) AS create_ast, parseQueryToJSON(\$\$SELECT * FROM $2\$\$) AS select_ast FORMAT TSVRaw" \
        | python3 -c "
import json, sys
create_ast, select_ast = (json.loads(part) for part in sys.stdin.read().rstrip('\n').split('\t'))
select = select_ast['list_of_selects']['children'][0]
table_function = select['tables']['children'][0]['table_expression']['table_function']
create_ast['storage']['engine']['arguments']['children'][1] = table_function
print(json.dumps(create_ast))
"
}

echo "--- carried in a Remote engine argument through the JSON AST dialect, it is refused too ---"
out=$({ json_create_query "$(engine_ast dst5_05060 "viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))")"
        ${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM ${db}.dst5_05060"; } 2>&1)
echo "$out" | grep -m1 -o "cannot be used to create a table"
echo "$out" | grep -m1 -o "UNKNOWN_TABLE"
echo "leaked: [$(echo "$out" | leaked)]"

echo "--- control: an engine argument that does not depend on grants is still accepted ---"
json_create_query "$(engine_ast dst6_05060 "numbers(10)")"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.dst6_05060"

echo "--- the ELSE fallback still works in a query, without the grant ---"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))"

${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.src_05060 TO $user"

echo "--- and the query returns the source rows once permitted ---"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM viewIfPermitted(SELECT secret_a FROM ${db}.src_05060 ELSE null('secret_a UInt64'))"

echo "--- control: a permitted user is still told about a structure mismatch ---"
out=$(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT count() FROM viewIfPermitted(SELECT * FROM ${db}.src_05060 ELSE null('x UInt64'))" 2>&1)
echo "$out" | grep -m1 -o "BAD_ARGUMENTS"
echo "leaked: [$(echo "$out" | leaked)]"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst1_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst2_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst3_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst4_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst5_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst6_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst7_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.dst8_05060"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.src_05060"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
