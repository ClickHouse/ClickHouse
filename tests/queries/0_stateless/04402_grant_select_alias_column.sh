#!/usr/bin/env bash
# Tags: no-old-analyzer

# Tests for column-level SELECT grants on ALIAS columns.
#
# 1. A user granted SELECT only on an ALIAS column must be able to read it,
#    including with a WHERE clause (regression test for the bug from
#    https://github.com/ClickHouse/ClickHouse/issues/31912 where a WHERE clause
#    turned the read into ACCESS_DENIED).
# 2. SELECT count() with a grant on only an ALIAS column must work and not throw
#    a "No available columns" LOGICAL_ERROR (the count reads the alias' physical
#    source columns, which is allowed even though the user cannot read them directly).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_alias (str String, sub_str String ALIAS substring(str, 1, 3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_alias(str) VALUES ('0123456')"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(sub_str) ON ${CLICKHOUSE_DATABASE}.test_alias TO ${USER}"

run() { ${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1"; }
denied() { ${CLICKHOUSE_CLIENT} --user "${USER}" --query "$1" 2>&1 | grep -o 'ACCESS_DENIED' | head -1; }

echo "-- read alias column"
run "SELECT sub_str FROM test_alias"
echo "-- read alias column with always-false WHERE"
run "SELECT sub_str FROM test_alias WHERE 1 = 0"
echo "-- read alias column with always-true WHERE"
run "SELECT sub_str FROM test_alias WHERE 1 = 1"
echo "-- read alias column with WHERE on the alias itself"
run "SELECT sub_str FROM test_alias WHERE sub_str = '012'"
echo "-- count() with a grant on only the alias column"
run "SELECT count() FROM test_alias"
echo "-- count() with always-false WHERE"
run "SELECT count() FROM test_alias WHERE 1 = 0"

echo "-- reading the underlying physical column directly is still denied"
denied "SELECT str FROM test_alias"
echo "-- referencing the physical column in WHERE is still denied"
denied "SELECT sub_str FROM test_alias WHERE str = '0123456'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_alias"
${CLICKHOUSE_CLIENT} --query "DROP USER ${USER}"
