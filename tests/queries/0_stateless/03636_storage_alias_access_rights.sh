#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

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

${CLICKHOUSE_CLIENT} --query "
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_alias TO ${username};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_table TO ${username};
"
echo "Test SELECT with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "SELECT count() FROM test_alias;"

# Test: INSERT
echo "Test INSERT still fails"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.test_alias TO ${username};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.test_table TO ${username};
"
echo "Test INSERT with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "INSERT INTO test_alias VALUES (4, 'four');"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: TRUNCATE
echo "Test TRUNCATE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT TRUNCATE ON ${CLICKHOUSE_DATABASE}.test_alias TO ${username};
    GRANT TRUNCATE ON ${CLICKHOUSE_DATABASE}.test_table TO ${username};
"
echo "Test TRUNCATE with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "TRUNCATE TABLE test_alias;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"

# Test: OPTIMIZE
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO test_table VALUES (5, 'five');
    REVOKE ALL ON ${CLICKHOUSE_DATABASE}.test_alias FROM ${username};
    REVOKE ALL ON ${CLICKHOUSE_DATABASE}.test_table FROM ${username};
"
echo "Test OPTIMIZE without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT OPTIMIZE ON ${CLICKHOUSE_DATABASE}.test_alias TO ${username};
    GRANT OPTIMIZE ON ${CLICKHOUSE_DATABASE}.test_table TO ${username};
"
echo "Test OPTIMIZE with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "OPTIMIZE TABLE test_alias FINAL;"

# Test: ALTER
echo "Test ALTER without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "
    GRANT ALL ON *.* TO ${username};
"
echo "Test ALTER with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "ALTER TABLE test_alias ADD COLUMN status String DEFAULT 'active';"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'test_table' AND name = 'status';"

${CLICKHOUSE_CLIENT} --query " REVOKE DROP ON *.* FROM ${username};"

# Test: DROP alias
echo "Test DROP alias without permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT DROP ON ${CLICKHOUSE_DATABASE}.test_alias TO ${username};"
echo "Test DROP alias with permission"
${CLICKHOUSE_CLIENT} --user="${username}" --query "DROP TABLE test_alias;"

# Verify target table still exists
echo "Test target table still exists"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_table;"
