#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

admin="${CLICKHOUSE_DATABASE}_admin"
user="${CLICKHOUSE_DATABASE}_u1"
p1="${CLICKHOUSE_DATABASE}_p1"
p2="${CLICKHOUSE_DATABASE}_p2"
p3="${CLICKHOUSE_DATABASE}_p3"
p_constrained="${CLICKHOUSE_DATABASE}_pc"
p_weak="${CLICKHOUSE_DATABASE}_pw"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${admin}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p1}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p2}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p3}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p_constrained}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p_weak}"

# Use max_execution_time because max_threads is randomized by the test framework
# and can violate constraints when connecting as a constrained user.

# Profile that sets max_execution_time=8
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${p1} SETTINGS max_execution_time = 8"
# Profile that sets max_execution_time=2
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${p2} SETTINGS max_execution_time = 2"
# Profile that inherits from p1 (which sets max_execution_time=8)
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${p3} SETTINGS INHERIT ${p1}"
# Profile with constraint: max_execution_time MAX 4
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${p_constrained} SETTINGS max_execution_time MAX 4"
# Profile with weaker constraint: max_execution_time MAX 8
${CLICKHOUSE_CLIENT} -q "CREATE SETTINGS PROFILE ${p_weak} SETTINGS max_execution_time MAX 8"

# Create an admin user who has constraints (max_execution_time MAX 4)
${CLICKHOUSE_CLIENT} -q "CREATE USER ${admin} SETTINGS PROFILE ${p_constrained}"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO ${admin} WITH GRANT OPTION"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE USER, ALTER USER, CREATE SETTINGS PROFILE ON *.* TO ${admin}"

# Test 1: Admin with max_execution_time MAX 4, tries to apply profile that sets max_execution_time=8 to another user -> should fail
echo "Test 1"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "ALTER USER ${user} SETTINGS PROFILE ${p1}" 2>&1 | grep -q "SETTING_CONSTRAINT_VIOLATION" && echo "VIOLATED" || echo "NO ERROR"

# Test 2: Admin with max_execution_time MAX 4, apply profile that sets max_execution_time=2 -> should succeed
echo "Test 2"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "ALTER USER ${user} SETTINGS PROFILE ${p2}"
echo "OK"

# Test 3: Profile inheriting from another profile that violates constraints -> should fail (recursive check)
echo "Test 3"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "ALTER USER ${user} SETTINGS PROFILE ${p3}" 2>&1 | grep -q "SETTING_CONSTRAINT_VIOLATION" && echo "VIOLATED" || echo "NO ERROR"

# Test 4: SET profile that violates own constraints -> should fail
echo "Test 4"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "SET profile = '${p1}'" 2>&1 | grep -q "SETTING_CONSTRAINT_VIOLATION" && echo "VIOLATED" || echo "NO ERROR"

# Test 5: SET profile that respects own constraints -> should succeed
echo "Test 5"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "SET profile = '${p2}'" && echo "OK" || echo "FAILED"

# Test 6: Admin (max_execution_time MAX 4) sets a value within its constraints AND makes it readonly in one
# operation -> should succeed (the value is allowed and making it CONST is tightening)
echo "Test 6"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "ALTER USER ${user} SETTINGS max_execution_time = 2 CONST" && echo "OK" || echo "FAILED"

# Test 7: Same, but the value violates the constraint -> should fail (the readonly declaration does not let the
# out-of-range value through; the value check fires)
echo "Test 7"
${CLICKHOUSE_CLIENT} --user="${admin}" -q "ALTER USER ${user} SETTINGS max_execution_time = 8 CONST" 2>&1 | grep -q "SETTING_CONSTRAINT_VIOLATION" && echo "VIOLATED" || echo "NO ERROR"

# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${admin}"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p1}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p2}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p3}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p_constrained}"
${CLICKHOUSE_CLIENT} -q "DROP SETTINGS PROFILE IF EXISTS ${p_weak}"
