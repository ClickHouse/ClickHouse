#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Parsing: expire_snapshots
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE db.t EXECUTE expire_snapshots(\'2024-06-01 00:00:00\')')"

# Parsing: other command names should parse successfully (generic EXECUTE syntax)
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE compact()')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE optimize_manifests()')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE some_future_command(\'arg1\', 42)')"
${CLICKHOUSE_CLIENT} --query "SELECT formatQuerySingleLine('ALTER TABLE t EXECUTE no_args_command()')"

# Runtime: EXECUTE on MergeTree should fail with NOT_IMPLEMENTED (use HTTP to bypass client-side parsing)
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_execute_03978"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_execute_03978 (x UInt32) ENGINE = MergeTree ORDER BY x"

${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "ALTER TABLE test_execute_03978 EXECUTE expire_snapshots('2024-06-01 00:00:00')" 2>&1 | grep -o 'NOT_IMPLEMENTED'
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "ALTER TABLE test_execute_03978 EXECUTE compact()" 2>&1 | grep -o 'NOT_IMPLEMENTED'
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "ALTER TABLE test_execute_03978 EXECUTE unknown_command()" 2>&1 | grep -o 'NOT_IMPLEMENTED'

# Privileges: ALTER TABLE EXECUTE is a child of ALTER TABLE
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "DROP USER IF EXISTS test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "CREATE USER test_user_03978"

# Verify that GRANT ALTER TABLE EXECUTE is valid and recorded correctly
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "GRANT ALTER TABLE EXECUTE ON *.* TO test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "SELECT access_type FROM system.grants WHERE user_name = 'test_user_03978' AND access_type = 'ALTER EXECUTE'"

# Verify that ALTER TABLE (parent) implicitly includes ALTER TABLE EXECUTE
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "REVOKE ALTER TABLE EXECUTE ON *.* FROM test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "GRANT ALTER TABLE ON *.* TO test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "SELECT 'has_alter_table' FROM system.grants WHERE user_name = 'test_user_03978' AND access_type = 'ALTER TABLE' LIMIT 1"

# Without any ALTER privilege, the command should be denied
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "REVOKE ALTER TABLE ON *.* FROM test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "GRANT SELECT ON *.* TO test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=test_user_03978" -d "ALTER TABLE test_execute_03978 EXECUTE expire_snapshots('2024-06-01 00:00:00')" 2>&1 | grep -o 'ACCESS_DENIED'

# Granting ALTER TABLE EXECUTE specifically should allow through (then hits NOT_IMPLEMENTED for MergeTree)
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "GRANT ALTER TABLE EXECUTE ON *.* TO test_user_03978"
${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}&user=test_user_03978" -d "ALTER TABLE test_execute_03978 EXECUTE expire_snapshots('2024-06-01 00:00:00')" 2>&1 | grep -o 'NOT_IMPLEMENTED'

${CLICKHOUSE_CURL} "${CLICKHOUSE_URL}" -d "DROP USER test_user_03978"
${CLICKHOUSE_CLIENT} --query "DROP TABLE test_execute_03978"
