#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: on a replicated / shared-catalog database the DDL runs with no user, so the
# access check asserted here is a no-op and the deny path silently allows.
# Blocked on https://github.com/ClickHouse/ClickHouse/issues/111561 - re-enable when fixed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04729_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user;
CREATE TABLE $db.protected_target (x UInt64, secret_column String) ENGINE = MergeTree ORDER BY x;
INSERT INTO $db.protected_target VALUES (42, 'shh');

CREATE USER $user;
-- The user may create tables in its own database, but starts without any rights on the destination
-- the engine points at, so the check below is exercised in isolation.
GRANT CREATE TABLE, DROP TABLE, SELECT, INSERT ON $db.* TO $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
GRANT TABLE ENGINE ON Buffer TO $user;
REVOKE SELECT, INSERT ON $db.protected_target FROM $user;
EOF

echo "-- 1. the destination's schema is not visible by the sanctioned route"
${CLICKHOUSE_CLIENT} --user "$user" --query "DESCRIBE $db.protected_target" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 1. omitted columns, no rights on the destination: rejected"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.b ENGINE = Buffer('$db', 'protected_target', 1, 10, 100, 10000, 1000000, 10000000, 100000000)" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 2. omitted columns, with SHOW COLUMNS: allowed and inferred from the destination"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON $db.protected_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.b ENGINE = Buffer('$db', 'protected_target', 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'b' ORDER BY position"

echo "-- 3. a short ATTACH still works after the grant is revoked"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON $db.protected_target FROM $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "DETACH TABLE $db.b"
${CLICKHOUSE_CLIENT} --user "$user" --query "ATTACH TABLE $db.b"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'b' ORDER BY position"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.b"

echo "-- 4. explicit columns, no rights on the destination: still allowed"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.b_explicit (x UInt64) ENGINE = Buffer('$db', 'protected_target', 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "SELECT name, type FROM system.columns WHERE database = '$db' AND table = 'b_explicit' ORDER BY position"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.b_explicit"

echo "-- 5. a temporary table with omitted columns is checked too"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TEMPORARY TABLE tmp_b ENGINE = Buffer('$db', 'protected_target', 1, 10, 100, 10000, 1000000, 10000000, 100000000)" 2>&1 \
    | grep -c -m1 "ACCESS_DENIED\|Not enough privileges"

echo "-- 6. SELECT on the destination implies SHOW COLUMNS, so reading users are unaffected"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.protected_target TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query \
    "CREATE TABLE $db.b_select ENGINE = Buffer('$db', 'protected_target', 1, 10, 100, 10000, 1000000, 10000000, 100000000)"
${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT x, secret_column FROM $db.b_select ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.b_select"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.protected_target"
