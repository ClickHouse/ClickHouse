#!/usr/bin/env bash

# Restoring a table with an external target, a MATERIALIZED VIEW's TO or a TimeSeries one, must
# authorize that target, like a CREATE does. Otherwise the restored object is an arbitrary write
# primitive: the forged backups below are built entirely from the user's own objects and name a
# target they hold no grant on.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_05055_${CLICKHOUSE_DATABASE}"
bk_forged="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_forged')"
bk_own="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_own')"
bk_inner="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inner')"
bk_ts="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_ts')"
bk_ext="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_ext')"
db_ext="${CLICKHOUSE_DATABASE}_ext"
db_restored="${CLICKHOUSE_DATABASE}_restored"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} --multiquery -q "
DROP TABLE IF EXISTS $db.mv_own;
DROP TABLE IF EXISTS $db.mv_inner;
DROP TABLE IF EXISTS $db.src;
DROP TABLE IF EXISTS $db.own;
DROP TABLE IF EXISTS $db.dst;
DROP TABLE IF EXISTS $db.metrics;
DROP DATABASE IF EXISTS $db_ext;
DROP DATABASE IF EXISTS $db_restored;
-- Its tables are resolved per file on demand and it iterates as empty, so a backup of it holds
-- only what the query renames into it, whatever the shared user_files directory contains.
CREATE DATABASE $db_ext ENGINE = Filesystem;
CREATE TABLE $db.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.own (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $db.metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE MATERIALIZED VIEW $db.mv_own TO $db.own AS SELECT k FROM $db.src;
CREATE MATERIALIZED VIEW $db.mv_inner ENGINE = MergeTree ORDER BY k AS SELECT k FROM $db.src;
CREATE USER $user;
GRANT CREATE VIEW ON $db.* TO $user;
GRANT SELECT ON $db.src TO $user;
GRANT SELECT, INSERT ON $db.own TO $user;
GRANT SELECT, INSERT ON $db.metrics TO $user;
GRANT BACKUP ON $db.mv_own TO $user;
GRANT BACKUP ON $db.mv_inner TO $user;
GRANT BACKUP ON $db.own TO $user;
GRANT BACKUP ON $db.metrics TO $user;
-- A Disk(...) backup location is authorized against the DISK source: writing a backup needs WRITE
-- ON DISK and reading one needs READ ON DISK. Neither half says anything about a view target.
GRANT READ ON DISK, WRITE ON DISK TO $user;
-- The inner-table arm needs this under table_engines_require_grant, on CREATE too. It confers no
-- SELECT or INSERT, so every denial below still comes from the view target.
GRANT TABLE ENGINE ON MergeTree TO $user;
-- Same for the TimeSeries arm: creating that table locally needs the engines and a temporary table.
GRANT TABLE ENGINE ON TimeSeries, TABLE ENGINE ON AggregatingMergeTree TO $user;
GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO $user;
GRANT CREATE DATABASE, CREATE TABLE, CREATE VIEW ON $db_restored.* TO $user;
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

echo "-- INSERT alone is not enough: the SELECT half of the requirement is pinned here"
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $db.dst TO $user"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_forged FROM $bk_forged FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "RESTORE TABLE $db.mv_own AS $db.mv_forged FROM $bk_forged FORMAT Null" 2>&1 \
    | grep -c -m1 'SELECT ON '"$db"'\.dst'
echo "-- and with both halves granted the same restore succeeds"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON $db.dst TO $user"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_forged FROM $bk_forged FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT, INSERT ON $db.dst FROM $user"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS $db.mv_forged"

echo "-- the direct CREATE path already requires the same grant"
deny_or_allow "CREATE MATERIALIZED VIEW $db.probe TO $db.dst AS SELECT k FROM $db.src"

echo "-- restoring a view whose target is granted is still allowed"
deny_or_allow "BACKUP TABLE $db.mv_own TO $bk_own FORMAT Null"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_ok FROM $bk_own FORMAT Null"

echo "-- a view with an inner table carries no target id, so it is still allowed"
deny_or_allow "BACKUP TABLE $db.mv_inner TO $bk_inner FORMAT Null"
deny_or_allow "RESTORE TABLE $db.mv_inner AS $db.mv_inner_ok FROM $bk_inner FORMAT Null"

# A TimeSeries table reaches the same primitive through its external targets, and a temporary one
# is checked on a separate branch of the restore gate, so it needs its own arm.
echo "-- a temporary TimeSeries table forges the same primitive through an external target"
if ${CLICKHOUSE_CLIENT} --user "$user" --multiquery -q "
    SET allow_experimental_time_series_table = 1;
    CREATE TEMPORARY TABLE ts ENGINE = TimeSeries METRICS $db.metrics;
    BACKUP TEMPORARY TABLE ts, TABLE $db.metrics AS $db.dst TO $bk_ts SETTINGS structure_only = 1 FORMAT Null;
    " > /dev/null 2>&1; then echo "forged"; else echo "forge failed"; fi
deny_or_allow "RESTORE TEMPORARY TABLE ts AS ts2 FROM $bk_ts SETTINGS structure_only = 1 FORMAT Null"
echo "-- and that denial names the forged target too"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "RESTORE TEMPORARY TABLE ts AS ts2 FROM $bk_ts SETTINGS structure_only = 1 FORMAT Null" 2>&1 \
    | grep -c -m1 'INSERT ON '"$db"'\.dst'

# A database with an external engine is skipped whole, so nothing under it is created and a view
# there can write nowhere. The same backup with the setting off does restore it, and does require
# the target, which is what keeps this pair from passing on an empty backup.
echo "-- a view under a database the restore skips does not require its target"
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $db_ext, TABLE $db.mv_own AS $db_ext.mv, TABLE $db.own AS $db.dst
    TO $bk_ext SETTINGS structure_only = 1 FORMAT Null"
deny_or_allow "RESTORE DATABASE $db_ext AS $db_restored FROM $bk_ext
    SETTINGS restore_replace_external_engines_to_null = 1, structure_only = 1 FORMAT Null"
echo "-- nothing was created, so the grant it used to demand was never needed"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '$db_restored'"
echo "-- with the setting off the same backup is restored, and then it does require the target"
deny_or_allow "RESTORE DATABASE $db_ext AS $db_restored FROM $bk_ext SETTINGS structure_only = 1 FORMAT Null"
echo "-- and the setting does not lift the check for a database that is not skipped"
deny_or_allow "RESTORE TABLE $db.mv_own AS $db.mv_forged3 FROM $bk_forged
    SETTINGS restore_replace_external_engines_to_null = 1 FORMAT Null"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS $db_restored"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS $db_ext"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
