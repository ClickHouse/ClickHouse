#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, long
# Tag no-parallel: Messes with internal cache
#     no-fasttest: Produces wrong results in fasttest, unclear why, didn't reproduce locally.
#     long: Sloooow ...

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# -- Attack 1:
#    - create a user,
#    - run a query whose result is stored in the query cache,
#    - drop the user, recreate it with the same name
#    - test that the cache entry is inaccessible

echo "Attack 1"

rnd=`tr -dc 1-9 </dev/urandom | head -c 5` # disambiguates the specific query in system.query_log below
# echo $rnd

# Start with empty query cache (QC).
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS admin"
${CLICKHOUSE_CLIENT} --query "CREATE USER admin"
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON *.* TO admin WITH GRANT OPTION"

# Insert cache entry
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT 0 == $rnd SETTINGS use_query_cache = 1"

# Check that the system view knows the new cache entry
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT 'system.query_cache with old user', count(*) FROM system.query_cache"

# Run query again. The 1st run must be a cache miss, the 2nd run a cache hit
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT 0 == $rnd SETTINGS use_query_cache = 1"
${CLICKHOUSE_CLIENT} --user "admin" --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query = 'SELECT 0 == $rnd SETTINGS use_query_cache = 1' order by event_time_microseconds"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS admin"
${CLICKHOUSE_CLIENT} --query "CREATE USER admin"
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON *.* TO admin WITH GRANT OPTION"

# Check that the system view reports the cache as empty
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT 'system.query_cache with new user', count(*) FROM system.query_cache"

# Run same query as old user. Expect a cache miss.
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT 0 == $rnd SETTINGS use_query_cache = 1"
${CLICKHOUSE_CLIENT} --user "admin" --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND query = 'SELECT 0 == $rnd SETTINGS use_query_cache = 1' order by event_time_microseconds"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP USER admin"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

# -- Attack 2: (scenario from issue #58054)
#    - create a user,
#    - create two roles, each with different row policies
#    - cached query result in the context of the 1st role must must not be visible in the context of the 2nd role

echo "Attack 2"

# Start with empty query cache (QC).
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS admin"
${CLICKHOUSE_CLIENT} --query "CREATE USER admin"
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS ON *.* TO admin WITH GRANT OPTION"

# Create table
${CLICKHOUSE_CLIENT} --user "admin" --query "DROP TABLE IF EXISTS user_data"
${CLICKHOUSE_CLIENT} --user "admin" --query "CREATE TABLE user_data (ID UInt32, userID UInt32) ENGINE = MergeTree ORDER BY userID"

# Create roles with row-level security

${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE IF EXISTS user_role_1"
# ${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE POLICY IF EXISTS user_policy_1"
${CLICKHOUSE_CLIENT} --user "admin" --query "CREATE ROLE user_role_1"
${CLICKHOUSE_CLIENT} --user "admin" --query "GRANT SELECT ON user_data TO user_role_1"
${CLICKHOUSE_CLIENT} --user "admin" --query "CREATE ROW POLICY user_policy_1 ON user_data FOR SELECT USING userID = 1 TO user_role_1"

${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE IF EXISTS user_role_2"
# ${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE POLICY IF EXISTS user_policy_2"
${CLICKHOUSE_CLIENT} --user "admin" --query "CREATE ROLE user_role_2"
${CLICKHOUSE_CLIENT} --user "admin" --query "GRANT SELECT ON user_data TO user_role_2"
${CLICKHOUSE_CLIENT} --user "admin" --query "CREATE ROW POLICY user_policy_2 ON user_data FOR SELECT USING userID = 2 TO user_role_2"

# Grant roles to admin
${CLICKHOUSE_CLIENT} --user "admin" --query "GRANT user_role_1, user_role_2 TO admin"
${CLICKHOUSE_CLIENT} --user "admin" --query "INSERT INTO user_data (ID, userID) VALUES (1, 1), (2, 2), (3, 1), (4, 3), (5, 2), (6, 1), (7, 4), (8, 2)"

# Test...
${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT '-- policy_1 test'"
${CLICKHOUSE_CLIENT} --user "admin" "SET ROLE user_role_1; SELECT * FROM user_data" # should only return rows with userID = 1

${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT '-- policy_2 test'"
${CLICKHOUSE_CLIENT} --user "admin" "SET ROLE user_role_2; SELECT * FROM user_data" # should only return rows with userID = 2

${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT '-- policy_1 with query cache test'"
${CLICKHOUSE_CLIENT} --user "admin" "SET ROLE user_role_1; SELECT * FROM user_data SETTINGS use_query_cache = 1" # should only return rows with userID = 1

${CLICKHOUSE_CLIENT} --user "admin" --query "SELECT '-- policy_2 with query cache test'"
${CLICKHOUSE_CLIENT} --user "admin" "SET ROLE user_role_2; SELECT * FROM user_data SETTINGS use_query_cache = 1" # should only return rows with userID = 2 (not userID = 1!)

# Cleanup
${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE user_role_1"
${CLICKHOUSE_CLIENT} --user "admin" --query "DROP ROLE user_role_2"
${CLICKHOUSE_CLIENT} --user "admin" --query "DROP TABLE user_data"
${CLICKHOUSE_CLIENT} --query "DROP USER admin"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP QUERY CACHE"
