#!/usr/bin/env bash
# Tags: no-replicated-database, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that GRANT CURRENT GRANTS correctly handles implicit privileges.
# For example, CREATE TABLE implicitly includes CREATE VIEW,
# so GRANT CURRENT GRANTS(CREATE VIEW) should work when user has CREATE TABLE.

user1="user04057_1_${CLICKHOUSE_DATABASE}_$RANDOM"
user2="user04057_2_${CLICKHOUSE_DATABASE}_$RANDOM"
role1="role04057_1_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} -n -q "
    DROP USER IF EXISTS $user1, $user2;
    DROP ROLE IF EXISTS $role1;
    CREATE USER $user1, $user2;
    CREATE ROLE $role1;
    GRANT CREATE TABLE, DROP TABLE, SELECT ON $db.* TO $role1 WITH GRANT OPTION;
    GRANT $role1 TO $user1;
"

# Test 1-4: GRANT CURRENT GRANTS with implicit privileges resolves correctly
echo "-- Test 1: GRANT CURRENT GRANTS(CREATE VIEW) with implicit privileges"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS(CREATE VIEW ON $db.*) TO $user2"
${CLICKHOUSE_CLIENT} -q "SHOW GRANTS FOR $user2" | sed 's/ TO.*//'
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM $user2"

echo "-- Test 2: GRANT CURRENT GRANTS(DROP VIEW) with implicit privileges"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS(DROP VIEW ON $db.*) TO $user2"
${CLICKHOUSE_CLIENT} -q "SHOW GRANTS FOR $user2" | sed 's/ TO.*//'
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM $user2"

echo "-- Test 3: GRANT CURRENT GRANTS with multiple implicit privileges"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS(CREATE VIEW, DROP VIEW, SELECT ON $db.*) TO $user2"
${CLICKHOUSE_CLIENT} -q "SHOW GRANTS FOR $user2" | sed 's/ TO.*//'
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM $user2"

echo "-- Test 4: GRANT CURRENT GRANTS (all) includes implicit privileges"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS ON $db.* TO $user2"
${CLICKHOUSE_CLIENT} -q "SHOW GRANTS FOR $user2" | sed 's/ TO.*//'
${CLICKHOUSE_CLIENT} -q "REVOKE ALL ON *.* FROM $user2"

# Test 5: user2 with no grants cannot CREATE VIEW or CREATE TABLE
echo "-- Test 5: CREATE VIEW and CREATE TABLE fail without grants"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE VIEW $db.test_view04057 AS SELECT 1; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE TABLE $db.test_table04057 (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }"

# Test 6: grant implicit CREATE VIEW via CURRENT GRANTS, verify CREATE VIEW works but CREATE TABLE does not
echo "-- Test 6: CREATE VIEW succeeds, CREATE TABLE fails with only CREATE VIEW grant"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS(CREATE VIEW, SELECT ON $db.*) TO $user2"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE VIEW $db.test_view04057 AS SELECT 1"
echo "CREATE VIEW: OK"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE TABLE $db.test_table04057 (x UInt8) ENGINE = MergeTree ORDER BY x; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} -n -q "
    DROP VIEW IF EXISTS $db.test_view04057;
    REVOKE ALL ON *.* FROM $user2;
"

# Test 7: grant implicit CREATE TABLE via CURRENT GRANTS, verify both CREATE TABLE and CREATE VIEW work
echo "-- Test 7: CREATE TABLE and CREATE VIEW both succeed with CREATE TABLE grant"
${CLICKHOUSE_CLIENT} --user $user1 -q "GRANT CURRENT GRANTS(CREATE TABLE, SELECT ON $db.*) TO $user2"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON MergeTree TO $user2"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE TABLE $db.test_table04057 (x UInt8) ENGINE = MergeTree ORDER BY x"
echo "CREATE TABLE: OK"
${CLICKHOUSE_CLIENT} --user $user2 -q "CREATE VIEW $db.test_view04057 AS SELECT 1"
echo "CREATE VIEW: OK"

# Cleanup
${CLICKHOUSE_CLIENT} -n -q "
    DROP VIEW IF EXISTS $db.test_view04057;
    DROP TABLE IF EXISTS $db.test_table04057;
    DROP USER IF EXISTS $user1, $user2;
    DROP ROLE IF EXISTS $role1;
"
