#!/usr/bin/env bash
# Tags: no-parallel

# Test database namespace isolation.
# When a user has DATABASE NAMESPACE set and the server has database_namespace_separator
# configured, all non-system database names are transparently prefixed with
# "{namespace}{separator}".

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique suffix to avoid conflicts
P="04006"

# Config file path for the server setting
NS_CONFIG="${CLICKHOUSE_CONFIG_DIR}/config.d/database_namespace_separator_${P}.xml"

# Cleanup function to ensure config is always removed
cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant1__testns" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant2__testns" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant1__otherdb" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant1__joindb" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant1__srcdb" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant3__altdb" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant1__sneakydb" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant1" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant2" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant3" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_nofeature" 2>/dev/null
    rm -f "${NS_CONFIG}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD CONFIG" 2>&1 | grep -v -e 'Listen .* failed'
}
trap cleanup EXIT

# Cleanup from previous runs (in case test was interrupted)
cleanup 2>/dev/null

# ============================================================
# Enable the database namespace feature via server setting
# ============================================================
cat > "${NS_CONFIG}" <<'EOF'
<clickhouse>
    <database_namespace_separator>__</database_namespace_separator>
</clickhouse>
EOF
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD CONFIG" 2>&1 | grep -v -e 'Listen .* failed'

# Verify the setting is active
${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.server_settings WHERE name = 'database_namespace_separator'"

# Create tenant users with DATABASE NAMESPACE
${CLICKHOUSE_CLIENT} -q "CREATE USER u_${P}_tenant1 DATABASE NAMESPACE tenant1"
${CLICKHOUSE_CLIENT} -q "CREATE USER u_${P}_tenant2 DATABASE NAMESPACE tenant2"

# Grant necessary privileges
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO u_${P}_tenant1"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO u_${P}_tenant2"

# Build a clean client command without --database for tenant users.
# The test framework's CLICKHOUSE_CLIENT includes --database=<test_db>, but that
# test database doesn't exist in the tenant's namespace. We use CLICKHOUSE_CLIENT_BINARY
# directly with only the port, to connect without specifying a database.
T1="${CLICKHOUSE_CLIENT_BINARY} --user u_${P}_tenant1 --port ${CLICKHOUSE_PORT_TCP}"
T2="${CLICKHOUSE_CLIENT_BINARY} --user u_${P}_tenant2 --port ${CLICKHOUSE_PORT_TCP}"

# ============================================================
# Test 1: CREATE DATABASE with namespace
# ============================================================
${T1} -q "CREATE DATABASE testns"

# Verify physical name exists in system.databases
${T1} -q "SELECT name FROM system.databases WHERE name = 'tenant1__testns'"

# ============================================================
# Test 2: CREATE TABLE and query with namespace
# ============================================================
${T1} -q "CREATE TABLE testns.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t1 VALUES (1), (2), (3)"
${T1} -q "SELECT * FROM testns.t1 ORDER BY x"

# ============================================================
# Test 3: USE database works with namespace
# ============================================================
${T1} -q "USE testns; SELECT currentDatabase()"

# ============================================================
# Test 4: SHOW DATABASES filters by namespace and strips prefix
# ============================================================
${T1} -q "SHOW DATABASES LIKE 'testns'"

# ============================================================
# Test 5: Tenant isolation — different namespace sees different databases
# ============================================================
${T2} -q "CREATE DATABASE testns"
${T2} -q "CREATE TABLE testns.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T2} -q "INSERT INTO testns.t1 VALUES (10), (20), (30)"
${T2} -q "SELECT * FROM testns.t1 ORDER BY x"

# Verify physical name
${T2} -q "SELECT name FROM system.databases WHERE name = 'tenant2__testns'"

# ============================================================
# Test 6: Switching back to tenant1 sees tenant1's data
# ============================================================
${T1} -q "SELECT * FROM testns.t1 ORDER BY x"

# ============================================================
# Test 7: System databases are not prefixed
# ============================================================
${T1} -q "SELECT count() > 0 FROM system.databases"

# ============================================================
# Test 8: DROP database works with namespace
# ============================================================
${T1} -q "CREATE DATABASE otherdb"
${T1} -q "SELECT name FROM system.databases WHERE name = 'tenant1__otherdb'"
${T1} -q "DROP DATABASE otherdb"
${T1} -q "SELECT count() FROM system.databases WHERE name = 'tenant1__otherdb'"

# ============================================================
# Test 9: Without namespace, physical names are visible
# ============================================================
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = 'tenant1__testns'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = 'tenant2__testns'"

# ============================================================
# Test 10: SHOW CREATE DATABASE strips namespace prefix
# ============================================================
# Output should contain 'testns' not 'tenant1__testns'
${T1} -q "SHOW CREATE DATABASE testns"

# ============================================================
# Test 11: SHOW CREATE TABLE strips namespace prefix
# ============================================================
# Database in output should be 'testns' not 'tenant1__testns'
${T1} -q "SHOW CREATE TABLE testns.t1"

# ============================================================
# Test 12: ALTER DATABASE with namespace
# ============================================================
${T1} -q "ALTER DATABASE testns MODIFY COMMENT 'tenant1 test database'"
${T1} -q "SELECT comment FROM system.databases WHERE name = 'tenant1__testns'"

# ============================================================
# Test 13: SHOW TABLES FROM with namespace
# ============================================================
${T1} -q "SHOW TABLES FROM testns"

# ============================================================
# Test 14: RENAME TABLE across databases within same namespace
# ============================================================
${T1} -q "CREATE DATABASE otherdb"
${T1} -q "RENAME TABLE testns.t1 TO otherdb.t1_moved"
${T1} -q "SELECT * FROM otherdb.t1_moved ORDER BY x"
${T1} -q "RENAME TABLE otherdb.t1_moved TO testns.t1"
${T1} -q "DROP DATABASE otherdb"

# ============================================================
# Test 15: EXISTS TABLE with namespace
# ============================================================
echo 'exists_table'
${T1} -q "EXISTS TABLE testns.t1"
${T1} -q "EXISTS TABLE testns.nonexistent"

# ============================================================
# Test 16: EXISTS DATABASE with namespace
# ============================================================
echo 'exists_database'
${T1} -q "EXISTS DATABASE testns"
${T1} -q "EXISTS DATABASE nonexistent_db"

# ============================================================
# Test 17: TRUNCATE TABLE with namespace
# ============================================================
echo 'truncate'
${T1} -q "CREATE TABLE testns.t_trunc (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_trunc VALUES (100), (200)"
${T1} -q "SELECT count() FROM testns.t_trunc"
${T1} -q "TRUNCATE TABLE testns.t_trunc"
${T1} -q "SELECT count() FROM testns.t_trunc"
${T1} -q "DROP TABLE testns.t_trunc"

# ============================================================
# Test 18: OPTIMIZE TABLE with namespace
# ============================================================
echo 'optimize'
${T1} -q "CREATE TABLE testns.t_opt (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_opt VALUES (1)"
${T1} -q "INSERT INTO testns.t_opt VALUES (2)"
${T1} -q "OPTIMIZE TABLE testns.t_opt FINAL"
${T1} -q "SELECT count() FROM testns.t_opt"
${T1} -q "DROP TABLE testns.t_opt"

# ============================================================
# Test 19: EXCHANGE TABLES with namespace
# ============================================================
echo 'exchange'
${T1} -q "CREATE TABLE testns.t_ex1 (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "CREATE TABLE testns.t_ex2 (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_ex1 VALUES (11)"
${T1} -q "INSERT INTO testns.t_ex2 VALUES (22)"
${T1} -q "EXCHANGE TABLES testns.t_ex1 AND testns.t_ex2"
${T1} -q "SELECT * FROM testns.t_ex1"
${T1} -q "SELECT * FROM testns.t_ex2"
${T1} -q "DROP TABLE testns.t_ex1"
${T1} -q "DROP TABLE testns.t_ex2"

# ============================================================
# Test 20: UNDROP TABLE with namespace
# ============================================================
echo 'undrop'
${T1} -q "SET database_atomic_wait_for_drop_and_detach_synchronously = 0; CREATE TABLE testns.t_undrop (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_undrop VALUES (42)"
${T1} -q "SET database_atomic_wait_for_drop_and_detach_synchronously = 0; DROP TABLE testns.t_undrop"
${T1} -q "SELECT table FROM system.dropped_tables WHERE database = 'tenant1__testns' AND table = 't_undrop' LIMIT 1"
${T1} -q "UNDROP TABLE testns.t_undrop"
${T1} -q "SELECT * FROM testns.t_undrop"
${T1} -q "DROP TABLE testns.t_undrop SYNC"

# ============================================================
# Test 21: SHOW CREATE USER shows DATABASE NAMESPACE
# ============================================================
echo 'show_create_user'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER u_${P}_tenant1" | grep -c 'DATABASE NAMESPACE tenant1'

# ============================================================
# Test 22: Default database behavior — tenant connects without
# specifying a database and can use the default database
# ============================================================
echo 'default_database'
${T1} -q "SELECT currentDatabase()"
${T1} -q "CREATE TABLE default.t_default_test (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO default.t_default_test VALUES (999)"
${T1} -q "SELECT * FROM default.t_default_test"
${T1} -q "DROP TABLE default.t_default_test"

# ============================================================
# Test 23: DESCRIBE TABLE with namespace
# ============================================================
echo 'describe'
${T1} -q "DESCRIBE TABLE testns.t1" | awk '{print $1, $2}'

# ============================================================
# Test 24: Cross-database JOIN within same namespace
# ============================================================
echo 'cross_db_join'
${T1} -q "CREATE DATABASE joindb"
${T1} -q "CREATE TABLE joindb.t2 (x UInt32, y String) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO joindb.t2 VALUES (1, 'one'), (2, 'two'), (3, 'three')"
${T1} -q "SELECT a.x, b.y FROM testns.t1 AS a JOIN joindb.t2 AS b ON a.x = b.x ORDER BY a.x"
${T1} -q "DROP DATABASE joindb"

# ============================================================
# Test 25: INSERT ... SELECT across namespaced databases
# ============================================================
echo 'insert_select'
${T1} -q "CREATE DATABASE srcdb"
${T1} -q "CREATE TABLE srcdb.src (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO srcdb.src VALUES (100), (200), (300)"
${T1} -q "CREATE TABLE testns.t_dest (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_dest SELECT * FROM srcdb.src"
${T1} -q "SELECT * FROM testns.t_dest ORDER BY x"
${T1} -q "DROP TABLE testns.t_dest"
${T1} -q "DROP DATABASE srcdb"

# ============================================================
# Test 26: CREATE VIEW on namespaced table
# ============================================================
echo 'view'
${T1} -q "CREATE VIEW testns.v1 AS SELECT x * 10 AS x10 FROM testns.t1"
${T1} -q "SELECT * FROM testns.v1 ORDER BY x10"
# SHOW CREATE should show view definition with un-namespaced db name
${T1} -q "SHOW CREATE TABLE testns.v1" | grep -o 'testns'
${T1} -q "DROP VIEW testns.v1"

# ============================================================
# Test 27: ATTACH/DETACH with namespace
# ============================================================
echo 'attach_detach'
${T1} -q "CREATE TABLE testns.t_ad (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T1} -q "INSERT INTO testns.t_ad VALUES (77)"
${T1} -q "DETACH TABLE testns.t_ad"
# Table should not be visible after detach
${T1} -q "EXISTS TABLE testns.t_ad"
${T1} -q "ATTACH TABLE testns.t_ad"
# Table should be back with data
${T1} -q "SELECT * FROM testns.t_ad"
${T1} -q "DROP TABLE testns.t_ad"

# ============================================================
# Test 28: Cross-tenant isolation — tenant1 cannot see tenant2's
# databases even by using the physical name
# ============================================================
echo 'cross_tenant_isolation'
# tenant1 tries to access "tenant2__testns" — this gets namespaced
# to "tenant1__tenant2__testns" which doesn't exist
${T1} -q "SELECT 1 FROM tenant2__testns.t1" 2>&1 | grep -o 'UNKNOWN_DATABASE'
# Verify tenant2's data is truly separate
${T2} -q "SELECT * FROM testns.t1 ORDER BY x"

# ============================================================
# Test 29: INFORMATION_SCHEMA access from tenant user
# ============================================================
echo 'information_schema'
# Tenant should be able to query INFORMATION_SCHEMA without namespace prefix
${T1} -q "SELECT count() > 0 FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'tenant1__testns'"
# Also lowercase version
${T1} -q "SELECT count() > 0 FROM information_schema.tables WHERE table_schema = 'tenant1__testns'"

# ============================================================
# Test 30: ALTER USER to change/remove namespace
# ============================================================
echo 'alter_user_namespace'
${CLICKHOUSE_CLIENT} -q "CREATE USER u_${P}_tenant3 DATABASE NAMESPACE tenant3"
${CLICKHOUSE_CLIENT} -q "GRANT ALL ON *.* TO u_${P}_tenant3"
T3="${CLICKHOUSE_CLIENT_BINARY} --user u_${P}_tenant3 --port ${CLICKHOUSE_PORT_TCP}"
# Create a database under tenant3 namespace
${T3} -q "CREATE DATABASE altdb"
${T3} -q "CREATE TABLE altdb.t1 (x UInt32) ENGINE = MergeTree() ORDER BY x"
${T3} -q "INSERT INTO altdb.t1 VALUES (333)"
${T3} -q "SELECT * FROM altdb.t1"
# Verify physical name
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = 'tenant3__altdb'"
# Change namespace to tenant3b
${CLICKHOUSE_CLIENT} -q "ALTER USER u_${P}_tenant3 DATABASE NAMESPACE tenant3b"
# Now the user sees tenant3b namespace — altdb is no longer visible
${T3} -q "EXISTS DATABASE altdb"
# Remove namespace entirely
${CLICKHOUSE_CLIENT} -q "ALTER USER u_${P}_tenant3 DATABASE NAMESPACE NONE"
# Now the user has no namespace — can see all databases by physical name
${T3} -q "SELECT count() FROM system.databases WHERE name = 'tenant3__altdb'"
# Cleanup
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS tenant3__altdb"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant3"

# ============================================================
# Test 31: Database name containing separator is rejected
# ============================================================
echo "separator_in_db_name"
# Any database name with the separator "__" should be rejected
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE tenant1__sneaky" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
# Namespaced user also can't use separator in their logical db name
${T1} -q "CREATE DATABASE bad__name" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
# But namespaced user CAN still create normal databases
${T1} -q "CREATE DATABASE sneakydb"
${T1} -q "SELECT count() FROM system.databases WHERE name = 'tenant1__sneakydb'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = 'tenant1__sneakydb'"
${T1} -q "DROP DATABASE sneakydb"

# ============================================================
# Test 32: Namespace value cannot contain separator
# ============================================================
echo "namespace_with_separator"
${CLICKHOUSE_CLIENT} -q "CREATE USER u_${P}_nofeature DATABASE NAMESPACE 'bad__ns'" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_nofeature"

# ============================================================
# Test 33: DATABASE NAMESPACE rejected when feature is disabled
# ============================================================
echo "feature_disabled"
# Remove the config to disable the feature
rm -f "${NS_CONFIG}"
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD CONFIG" 2>&1 | grep -v -e 'Listen .* failed'
# Now trying to create a user with namespace should fail
${CLICKHOUSE_CLIENT} -q "CREATE USER u_${P}_nofeature DATABASE NAMESPACE somens" 2>&1 | grep -m1 -o 'BAD_ARGUMENTS'
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_nofeature"
# Re-enable for cleanup (cleanup trap needs it to drop namespaced databases)
cat > "${NS_CONFIG}" <<'EOF'
<clickhouse>
    <database_namespace_separator>__</database_namespace_separator>
</clickhouse>
EOF
${CLICKHOUSE_CLIENT} -q "SYSTEM RELOAD CONFIG" 2>&1 | grep -v -e 'Listen .* failed'

# ============================================================
# Cleanup (also handled by trap)
# ============================================================
${T1} -q "DROP DATABASE IF EXISTS testns"
${T2} -q "DROP DATABASE IF EXISTS testns"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant1"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS u_${P}_tenant2"
