#!/usr/bin/env bash
# Regression for https://github.com/ClickHouse/ClickHouse/issues/101357
# GRANT role TO itself must be rejected with BAD_ARGUMENTS (code 36).
# Roles are global in ClickHouse, so suffix names with ${CLICKHOUSE_DATABASE}
# to avoid collisions between parallel test workers.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

role_a="test_role_04109_a_${CLICKHOUSE_DATABASE}"
role_b="test_role_04109_b_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS ${role_a}, ${role_b}"
$CLICKHOUSE_CLIENT -q "CREATE ROLE ${role_a}"
$CLICKHOUSE_CLIENT -q "CREATE ROLE ${role_b}"

# Direct self-grant.
$CLICKHOUSE_CLIENT -q "GRANT ${role_a} TO ${role_a}" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1

# Multi-role grant with one self-grant must still be rejected.
$CLICKHOUSE_CLIENT -q "GRANT ${role_a}, ${role_b} TO ${role_a}" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -n1

# Legitimate role-to-role grant still works.
$CLICKHOUSE_CLIENT -q "GRANT ${role_a} TO ${role_b}"

# Nothing has been granted to ${role_a} as a consequence.
$CLICKHOUSE_CLIENT -q "SHOW GRANTS FOR ${role_a}"

$CLICKHOUSE_CLIENT -q "DROP ROLE ${role_a}, ${role_b}"
