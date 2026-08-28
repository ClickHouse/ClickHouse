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
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${db}.src_05060"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
