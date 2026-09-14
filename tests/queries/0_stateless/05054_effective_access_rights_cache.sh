#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for EffectiveAccessRightsCache: the effective access rights of a user
# are shared between the sessions of that user, and GRANT/REVOKE must be immediately
# visible to already established sessions (guards against stale cache results).
#
# Bounds on the numbers of calculations and cache hits are asserted per query via
# system.query_log: the notification handler runs synchronously in the thread of the query
# that changed the user, so the rebuilds land in that query's own ProfileEvents.

TEST_USER=test_user_effective_access_cache
TEST_USER2=test_user2_effective_access_cache
TEST_USER3=test_user3_effective_access_cache
TEST_DB=test_db_effective_access_cache
TEST_DB2=test_db2_effective_access_cache
TEST_DB3=test_db3_effective_access_cache
TEST_ROLE=test_role_effective_access_cache
TEST_ROLE_A=test_role_a_effective_access_cache
TEST_ROLE_B=test_role_b_effective_access_cache
QID_PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_earc"

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

# An established session must see rights granted and revoked after it was established.
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

# Two sessions with different parameters (different current_database, i.e. different
# ContextAccess objects) but the same user and roles: after a GRANT only one of them
# recalculates and the other reuses the cached result.
$CLICKHOUSE_CLIENT -q "CREATE TABLE $TEST_DB.t3 (a UInt64) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER2"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -q "SELECT 1"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -d $TEST_DB2 -q "SELECT 1"
$CLICKHOUSE_CLIENT --query_id ${QID_PREFIX}_g -q "GRANT SELECT ON $TEST_DB.t3 TO $TEST_USER2"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -q "SELECT count() FROM $TEST_DB.t3"
$CLICKHOUSE_CLIENT --user $TEST_USER2 -d $TEST_DB2 -q "SELECT count() FROM $TEST_DB.t3"

# Sessions of the same user using different role sets must each see their own rights,
# and each role set must keep its own cache entry. The four sessions below use the role
# sets A, B, A, B over four different databases (i.e. four different ContextAccess
# objects): a single entry per user instead of one per role set would make the second A
# and the second B recalculate at the GRANT below, exceeding the bound asserted at the
# end.
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
$CLICKHOUSE_CLIENT --query_id ${QID_PREFIX}_g3 -q "GRANT SELECT ON $TEST_DB.t3 TO $TEST_USER3"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -q "SET ROLE $TEST_ROLE_A" -q "SELECT count() FROM $TEST_DB.t3"
$CLICKHOUSE_CLIENT --user $TEST_USER3 -d $TEST_DB -q "SET ROLE $TEST_ROLE_B" -q "SELECT count() FROM $TEST_DB.t2"
if $CLICKHOUSE_CLIENT --user $TEST_USER3 -q "SET ROLE $TEST_ROLE_A" -q "SELECT count() FROM $TEST_DB.t2" 2>&1 | grep -q "ACCESS_DENIED"; then
    echo "revoked"
else
    echo "unexpected"
fi

# The numbers of calculations and cache hits can only be asserted for the GRANT queries
# themselves: a session's own access is calculated at login (Session::authenticate ->
# getUser) and at query context creation (makeQueryContext -> getAccess), before the
# query's QueryScope establishes its ProfileEvents, so those numbers reach only the
# global counters. A GRANT rebuilds every live session of the user synchronously in its
# own thread, so its rebuilds do land in the GRANT's own ProfileEvents.
#
# Only bounds are asserted, not exact counts: how many ContextAccess objects the sessions
# translate into (one per login, one per query context, deduplicated or not) is an
# implementation detail this test must not depend on. Whatever that number is, all
# sessions of TEST_USER2 share its single role set, so its GRANT triggers at most one
# calculation, and the sessions after the first reuse it (at least one cache hit).
# A missing or per-session cache would recalculate once per session and exceed the
# bound; the cache hit requirement fails the assertion if there were no live sessions
# to share anything between.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['EffectiveAccessRightsCalculations'] <= 1
       AND ProfileEvents['EffectiveAccessRightsCacheHits'] >= 1
    FROM system.query_log
    WHERE query_id = '${QID_PREFIX}_g' AND type = 'QueryFinish'"

# The sessions of TEST_USER3 can only use three role sets: {A}, {B}, and the default
# {A, B}. Each role set is calculated at most once per GRANT, so at most three
# calculations happen in total, and the sessions after the first of a role set reuse
# it (at least one cache hit). A single entry per user instead of one per role set
# would recalculate on every role-set switch of the rebuild and exceed the bound.
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['EffectiveAccessRightsCalculations'] <= 3
       AND ProfileEvents['EffectiveAccessRightsCacheHits'] >= 1
    FROM system.query_log
    WHERE query_id = '${QID_PREFIX}_g3' AND type = 'QueryFinish'"

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
