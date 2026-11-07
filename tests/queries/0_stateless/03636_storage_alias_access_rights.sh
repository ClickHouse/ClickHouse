#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# An `Alias` runs with definer rights: a user needs the privilege for the operation on the alias
# itself, never on the target. The alias below is created by the default user, who becomes the definer
# and holds full access to the target, so forwarded operations succeed with only an alias-level grant.
${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS test_table;
    DROP TABLE IF EXISTS test_alias;

    CREATE TABLE test_table (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO test_table VALUES (1, 'one'), (2, 'two'), (3, 'three');

    CREATE TABLE test_alias ENGINE = Alias('test_table');
    CREATE USER ${username} NOT IDENTIFIED;
"

# Test: SELECT
echo "Test SELECT without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT * FROM test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

echo "Test INSERT without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

# A grant on the alias alone is enough; the target is never granted to the user below.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON test_alias TO ${username};"
echo "Test SELECT with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM test_alias;"

# Test: INSERT
echo "Test INSERT still fails"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON test_alias TO ${username};"
echo "Test INSERT with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: TRUNCATE
echo "Test TRUNCATE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT TRUNCATE ON test_alias TO ${username};"
echo "Test TRUNCATE with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: OPTIMIZE
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_table VALUES (5, 'five');"
echo "Test OPTIMIZE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT OPTIMIZE ON test_alias TO ${username};"
echo "Test OPTIMIZE with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias FINAL;"

# Test: CHECK TABLE requires CHECK on the alias only.
echo "Test CHECK TABLE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "CHECK TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT CHECK ON test_alias TO ${username};"
echo "Test CHECK TABLE with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SET check_query_single_value_result = 1; CHECK TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --user="${username}" --query "SET check_query_single_value_result = 1; CHECK TABLE test_alias;"

# Test: ALTER
echo "Test ALTER without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT ALTER ON test_alias TO ${username};"
echo "Test ALTER with alias permission only"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'test_table' AND name = 'status';"

# Test: DROP alias
echo "Test DROP alias without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT DROP ON test_alias TO ${username};"
echo "Test DROP alias with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;"

# Verify target table still exists
echo "Test target table still exists"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP TABLE IF EXISTS test_table;
"
