#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for `EffectiveAccessRightsCache`: compatible access contexts can reuse
# effective access rights, and subsequent queries must see `GRANT`/`REVOKE` changes.
# Each client invocation opens a new connection; the existing context cache can retain
# access contexts between invocations. Persistent HTTP sessions are covered separately
# by `05055_effective_access_rights_role_permissions.sh`.
#
# Cache calculation and hit counts are checked on an isolated server in
# `tests/integration/test_effective_access_rights_cache`.

TEST_USER=test_user_effective_access_cache
TEST_USER2=test_user2_effective_access_cache
TEST_USER3=test_user3_effective_access_cache
TEST_DB=test_db_effective_access_cache
TEST_DB2=test_db2_effective_access_cache
TEST_DB3=test_db3_effective_access_cache
TEST_ROLE=test_role_effective_access_cache
TEST_ROLE_A=test_role_a_effective_access_cache
TEST_ROLE_B=test_role_b_effective_access_cache

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TEST_USER"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TEST_USER2"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TEST_USER3"
$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $TEST_ROLE"
$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $TEST_ROLE_A"
$CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $TEST_ROLE_B"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $TEST_DB"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $TEST_DB2"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $TEST_DB3"
$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $TEST_DB"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $TEST_DB2"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $TEST_DB3"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TEST_DB.t1 (a UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TEST_DB.t2 (a UInt64) ENGINE = Memory"

# Subsequent connections must see updated rights even when an access context is cached.
$CLICKHOUSE_CLIENT --user $TEST_USER -q "SELECT 1"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t1 TO $TEST_USER"
$CLICKHOUSE_CLIENT --user $TEST_USER -q "SELECT count() FROM $TEST_DB.t1"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON $TEST_DB.t1 FROM $TEST_USER"
if $CLICKHOUSE_CLIENT --user $TEST_USER -q "SELECT count() FROM $TEST_DB.t1" 2>&1 | grep -q "ACCESS_DENIED"; then
    echo "revoked"
else
    echo "unexpected"
fi

# Rights coming from a role must be visible as well.
$CLICKHOUSE_CLIENT -q "CREATE ROLE $TEST_ROLE"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t2 TO $TEST_ROLE"
$CLICKHOUSE_CLIENT -q "GRANT $TEST_ROLE TO $TEST_USER"
$CLICKHOUSE_CLIENT --user $TEST_USER -q "SELECT count() FROM $TEST_DB.t2"
$CLICKHOUSE_CLIENT -q "REVOKE $TEST_ROLE FROM $TEST_USER"
if $CLICKHOUSE_CLIENT --user $TEST_USER -q "SELECT count() FROM $TEST_DB.t2" 2>&1 | grep -q "ACCESS_DENIED"; then
    echo "revoked"
else
    echo "unexpected"
fi

# Different current databases create distinct `ContextAccess` objects for the same user
# and roles. During this test's sequential `GRANT` notifications, the contexts can reuse
# a result calculated for the updated user snapshot.
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TEST_DB.t3 (a UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER2"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -q "SELECT 1"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -d $TEST_DB2 -q "SELECT 1"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t3 TO $TEST_USER2"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -q "SELECT count() FROM $TEST_DB.t3"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -d $TEST_DB2 -q "SELECT count() FROM $TEST_DB.t3"

# Connections using different role sets must each see their own rights. The four client
# invocations below select role sets A, B, A, B with different current databases, creating
# distinct access contexts. Separate cache entries for the enabled-role snapshots allow
# each group of compatible contexts to reuse results during the subsequent `GRANT`.
$CLICKHOUSE_CLIENT -q "CREATE ROLE $TEST_ROLE_A"
$CLICKHOUSE_CLIENT -q "CREATE ROLE $TEST_ROLE_B"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t1 TO $TEST_ROLE_A"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t2 TO $TEST_ROLE_B"
$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER3"
$CLICKHOUSE_CLIENT -q "GRANT $TEST_ROLE_A, $TEST_ROLE_B TO $TEST_USER3"

$CLICKHOUSE_CLIENT --user $TEST_USER3 -q "SET ROLE $TEST_ROLE_A" -q "SELECT 1"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -d $TEST_DB -q "SET ROLE $TEST_ROLE_B" -q "SELECT 1"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -d $TEST_DB2 -q "SET ROLE $TEST_ROLE_A" -q "SELECT 1"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -d $TEST_DB3 -q "SET ROLE $TEST_ROLE_B" -q "SELECT 1"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t3 TO $TEST_USER3"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -q "SET ROLE $TEST_ROLE_A" -q "SELECT count() FROM $TEST_DB.t3"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -d $TEST_DB -q "SET ROLE $TEST_ROLE_B" -q "SELECT count() FROM $TEST_DB.t2"
if $CLICKHOUSE_CLIENT --user $TEST_USER3 -q "SET ROLE $TEST_ROLE_A" -q "SELECT count() FROM $TEST_DB.t2" 2>&1 | grep -q "ACCESS_DENIED"; then
    echo "revoked"
else
    echo "unexpected"
fi

$CLICKHOUSE_CLIENT -q "DROP ROLE $TEST_ROLE_A"
$CLICKHOUSE_CLIENT -q "DROP ROLE $TEST_ROLE_B"
$CLICKHOUSE_CLIENT -q "DROP ROLE $TEST_ROLE"
$CLICKHOUSE_CLIENT -q "DROP USER $TEST_USER"
$CLICKHOUSE_CLIENT -q "DROP USER $TEST_USER2"
$CLICKHOUSE_CLIENT -q "DROP USER $TEST_USER3"
$CLICKHOUSE_CLIENT -q "DROP TABLE $TEST_DB.t1"
$CLICKHOUSE_CLIENT -q "DROP TABLE $TEST_DB.t2"
$CLICKHOUSE_CLIENT -q "DROP TABLE $TEST_DB.t3"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $TEST_DB"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $TEST_DB2"
$CLICKHOUSE_CLIENT -q "DROP DATABASE $TEST_DB3"
