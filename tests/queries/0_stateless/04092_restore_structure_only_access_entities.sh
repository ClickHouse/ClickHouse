#!/usr/bin/env bash
# Tags: no-parallel

# Test that structure_only=true restores access entities (users, roles) and UDFs,
# but does not restore table data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_name="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
role_name="role_${CLICKHOUSE_TEST_UNIQUE_NAME}"
func_name="func_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_name};
DROP ROLE IF EXISTS ${role_name};
DROP FUNCTION IF EXISTS ${func_name};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.test_data;

CREATE ROLE ${role_name};
CREATE USER ${user_name} DEFAULT ROLE ${role_name};
CREATE FUNCTION ${func_name} AS (x) -> x + 1;

CREATE TABLE ${CLICKHOUSE_DATABASE}.test_data (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.test_data VALUES (1, 'hello'), (2, 'world');
"

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} --query "BACKUP DATABASE ${CLICKHOUSE_DATABASE}, TABLE system.users, TABLE system.roles, TABLE system.functions TO ${backup_name} FORMAT Null"

# Drop everything
${CLICKHOUSE_CLIENT} -m --query "
DROP USER ${user_name};
DROP ROLE ${role_name};
DROP FUNCTION ${func_name};
DROP TABLE ${CLICKHOUSE_DATABASE}.test_data;
"

# Restore with structure_only - should restore table structure, access entities, and UDFs, but not table data
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} SETTINGS structure_only=true, allow_non_empty_tables=true FORMAT Null"

# Verify: table exists but has no data
echo "table_exists"
${CLICKHOUSE_CLIENT} --query "EXISTS ${CLICKHOUSE_DATABASE}.test_data"
echo "row_count"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.test_data"

# Verify: user and role were restored
echo "user_exists"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.users WHERE name = '${user_name}'"
echo "role_exists"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.roles WHERE name = '${role_name}'"

# Verify: UDF was restored
echo "function_exists"
${CLICKHOUSE_CLIENT} --query "SELECT ${func_name}(42)"

# Cleanup
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user_name};
DROP ROLE IF EXISTS ${role_name};
DROP FUNCTION IF EXISTS ${func_name};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.test_data;
"
