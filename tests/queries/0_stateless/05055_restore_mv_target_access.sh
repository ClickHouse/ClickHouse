#!/usr/bin/env bash

# Restoring a MATERIALIZED VIEW with an external TO target must authorize that target, like a CREATE does.
# Otherwise the restored view is an arbitrary write primitive: the forged backup below is built entirely
# from the user's own objects and names a target they hold no grant on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_05055_${CLICKHOUSE_DATABASE}"
bk_forged="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_forged')"
bk_own="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_own')"
bk_inner="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inner')"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --multiquery -q "
DROP TABLE IF EXISTS $db.mv_own;
DROP TABLE IF EXISTS $db.mv_inner;
DROP TABLE IF EXISTS $db.src;
DROP TABLE IF EXISTS $db.own;
DROP TABLE IF EXISTS $db.dst;
CREATE TABLE $db.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.own (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW $db.mv_own TO $db.own AS SELECT k FROM $db.src;
CREATE MATERIALIZED VIEW $db.mv_inner ENGINE = MergeTree ORDER BY k AS SELECT k FROM $db.src;
CREATE USER $user;
GRANT CREATE VIEW ON $db.* TO $user;
GRANT SELECT ON $db.src TO $user;
GRANT SELECT, INSERT ON $db.own TO $user;
GRANT BACKUP ON $db.mv_own TO $user;
GRANT BACKUP ON $db.mv_inner TO $user;
GRANT BACKUP ON $db.own TO $user;
-- The inner-table arm needs this under table_engines_require_grant, on CREATE too. It confers no
-- SELECT or INSERT, so every denial below still comes from the view target.
GRANT TABLE ENGINE ON MergeTree TO $user;
"

deny_or_allow() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "allowed"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then echo "denied"
    else echo "unexpected: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"; fi
}

echo "-- forging the backup needs no privilege on the target it renames the view onto"
deny_or_allow "BACKUP TABLE $db.mv_own, TABLE $db.own AS $db.dst TO $bk_forged FORMAT Null"

echo "-- restoring the forged view is denied"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_forged FROM $bk_forged FORMAT Null"
echo "-- and the denial names the target, not some unrelated grant"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "RESTORE TABLE $db.mv_own AS $db.mv_forged FROM $bk_forged FORMAT Null" 2>&1 \
    | grep -c -m1 'INSERT ON '"$db"'\.dst'
echo "-- the view was not created by the rejected restore"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'mv_forged'"

echo "-- the direct CREATE path already requires the same grant"
deny_or_allow "CREATE MATERIALIZED VIEW $db.probe TO $db.dst AS SELECT k FROM $db.src"

echo "-- restoring a view whose target is granted is still allowed"
deny_or_allow "BACKUP TABLE $db.mv_own TO $bk_own FORMAT Null"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_ok FROM $bk_own FORMAT Null"

echo "-- a view with an inner table carries no target id, so it is still allowed"
deny_or_allow "BACKUP TABLE $db.mv_inner TO $bk_inner FORMAT Null"
deny_or_allow "RESTORE TABLE $db.mv_inner AS $db.mv_inner_ok FROM $bk_inner FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
