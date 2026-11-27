#!/usr/bin/env bash
# Regression test for issue #107530: authenticatedUser() must evaluate to the
# originally authenticated user (not an empty string, and not currentUser()) on the
# async_insert flush path. Uses EXECUTE AS so currentUser() and authenticatedUser()
# diverge: the test then fails if setAuthenticatedUserName is removed (au becomes empty)
# or wired to the wrong value (au becomes the impersonated user).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

AUTH_USER="auth_user_${CLICKHOUSE_DATABASE}"
APP_USER="app_user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${AUTH_USER}"
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${APP_USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${AUTH_USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "CREATE USER ${APP_USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT IMPERSONATE ON ${APP_USER} TO ${AUTH_USER}"
$CLICKHOUSE_CLIENT --query "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${AUTH_USER}"
$CLICKHOUSE_CLIENT --query "GRANT ALL ON ${CLICKHOUSE_DATABASE}.* TO ${APP_USER}"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_auth_user"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_auth_user_src"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_auth_user_dst"
$CLICKHOUSE_CLIENT --query "DROP VIEW IF EXISTS t_auth_user_mv"

# DEFAULT columns: currentUser() = impersonated user, authenticatedUser() = original user.
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_auth_user
(
    x Int32,
    cu String DEFAULT currentUser(),
    au String DEFAULT authenticatedUser()
)
ENGINE = MergeTree ORDER BY x"

# Async insert (x=1) and sync insert (x=2), both under EXECUTE AS the impersonated user.
$CLICKHOUSE_CLIENT --user "${AUTH_USER}" --query \
    "EXECUTE AS ${APP_USER}; INSERT INTO ${CLICKHOUSE_DATABASE}.t_auth_user (x) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1)"
$CLICKHOUSE_CLIENT --user "${AUTH_USER}" --query \
    "EXECUTE AS ${APP_USER}; INSERT INTO ${CLICKHOUSE_DATABASE}.t_auth_user (x) SETTINGS async_insert = 0                            VALUES (2)"

# The async row (x=1) and the sync row (x=2) must both record currentUser()=impersonated
# and authenticatedUser()=originally authenticated. Before the fix the async row recorded
# an empty authenticatedUser(). cu_ne_au proves the two ClientInfo fields are restored
# independently (it would be false if authenticatedUser were mis-wired to currentUser).
$CLICKHOUSE_CLIENT --query "
SELECT
    x,
    cu = '${APP_USER}' AS cu_is_impersonated,
    au = '${AUTH_USER}' AS au_is_authenticated,
    cu != au AS cu_ne_au
FROM t_auth_user ORDER BY x"

# Materialized view selecting authenticatedUser() under EXECUTE AS on the async path.
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_auth_user_src (x Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_auth_user_dst (x Int32, cu String, au String) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW t_auth_user_mv TO t_auth_user_dst AS SELECT x, currentUser() AS cu, authenticatedUser() AS au FROM t_auth_user_src"

$CLICKHOUSE_CLIENT --user "${AUTH_USER}" --query \
    "EXECUTE AS ${APP_USER}; INSERT INTO ${CLICKHOUSE_DATABASE}.t_auth_user_src SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (10)"

$CLICKHOUSE_CLIENT --query "
SELECT
    x,
    cu = '${APP_USER}' AS cu_is_impersonated,
    au = '${AUTH_USER}' AS au_is_authenticated,
    cu != au AS cu_ne_au
FROM t_auth_user_dst ORDER BY x"

$CLICKHOUSE_CLIENT --query "DROP VIEW t_auth_user_mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_auth_user_dst"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_auth_user_src"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_auth_user"
$CLICKHOUSE_CLIENT --query "DROP USER ${AUTH_USER}"
$CLICKHOUSE_CLIENT --query "DROP USER ${APP_USER}"
