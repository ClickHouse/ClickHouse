#!/usr/bin/env bash
# Tags: no-encrypted-storage, no-replicated-database
# no-replicated-database: the CREATE DATABASE runs with no user there, so its check is a no-op.

# BACKUP/RESTORE and `ENGINE = Backup` must authorize the backup location against the SOURCES grant
# model, like the matching table functions do. Without the fix, a user with SOURCES revoked can write
# a backup to (and read one from) any location the server can reach.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04774_${CLICKHOUSE_DATABASE}"
src="${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
bk="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b')"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS $src"
${CLICKHOUSE_CLIENT} --multiquery -q "
CREATE DATABASE $src;
CREATE TABLE $src.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $src.t VALUES (42);
CREATE USER $user;
GRANT BACKUP ON $src.* TO $user;
GRANT CREATE DATABASE, DROP DATABASE ON *.* TO $user;
GRANT CREATE TABLE, INSERT, SELECT ON $db.* TO $user;
GRANT SELECT ON $src.* TO $user;
GRANT SELECT ON ${db}_b2.* TO $user;
-- Needed under table_engines_require_grant; the wildcard is the only spelling that reaches the
-- Backup DATABASE engine. It confers no SOURCES, so every denial below still comes from SOURCES.
GRANT TABLE ENGINE ON *.* TO $user;
REVOKE SOURCES ON *.* FROM $user;
"
# An admin-made backup the restricted user will try to read.
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $src TO $bk FORMAT Null"

deny_or_allow() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "allowed"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then echo "denied"
    else echo "unexpected: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"; fi
}

# For a case whose expected outcome is a specific error other than an access denial.
deny_or_code() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "allowed"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then echo "denied"
    else echo "$out" | grep -oE 'Code: [0-9]+' | head -1; fi
}

echo "-- BACKUP without WRITE ON FILE: denied, and it names the missing grant"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_denied') FORMAT Null" 2>&1 \
    | grep -c -m1 'WRITE ON FILE'
echo "-- BACKUP with WRITE ON FILE: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO $user"
deny_or_allow "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_ok') FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON FILE FROM $user"

echo "-- RESTORE without READ ON FILE: denied"
deny_or_allow "RESTORE TABLE $src.t AS $db.r1 FROM $bk FORMAT Null"
echo "-- the table was not created by the rejected restore"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'r1'"
echo "-- RESTORE with READ ON FILE: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.r2 FROM $bk FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM $db.r2"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON FILE FROM $user"

echo "-- CREATE DATABASE ENGINE = Backup without READ ON FILE: denied"
deny_or_allow "CREATE DATABASE ${db}_b1 ENGINE = Backup('$src', $bk)"
echo "-- the database was not created"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${db}_b1'"
echo "-- CREATE DATABASE ENGINE = Backup with READ ON FILE: allowed and readable"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO $user"
deny_or_allow "CREATE DATABASE ${db}_b2 ENGINE = Backup('$src', $bk)"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM ${db}_b2.t"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON FILE FROM $user"

echo "-- WRITE ON FILE alone does not authorize reading a backup"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.r3 FROM $bk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON FILE FROM $user"

echo "-- re-attaching an already-authorized database does not re-demand the grant"
${CLICKHOUSE_CLIENT} --user "$user" -q "DETACH DATABASE ${db}_b2"
deny_or_allow "ATTACH DATABASE ${db}_b2"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM ${db}_b2.t"

echo "-- a full definition is a new definition, so it is still validated"
deny_or_allow "ATTACH DATABASE ${db}_b4 ENGINE = Backup('$src', $bk)"

echo "-- restoring over an existing Backup-engine database does not demand the inner locator"
# An existing database makes creation a no-op (create_database='if not exists') or an error
# (create_database='create'), so the definition found in the backup is never constructed and must
# not be authorized either. Under 'create' the pre-existing DATABASE_ALREADY_EXISTS must survive.
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO $user"
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE ${db}_b2 TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_outer') FORMAT Null"
deny_or_allow "RESTORE DATABASE ${db}_b2 FROM File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_outer') FORMAT Null"
echo "-- under create_database='create' the pre-existing DATABASE_ALREADY_EXISTS survives"
deny_or_code "RESTORE DATABASE ${db}_b2 FROM File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_outer') SETTINGS create_database='create' FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON FILE FROM $user"

echo "-- an explicit base_backup locator is authorized for reading, not for the outer direction"
# The outer locator is written, so only the base can demand READ. A whole-source FILE grant cannot
# distinguish the two locators, which is why the informative arm is the incremental BACKUP one: with
# WRITE granted, a denial can only come from the base.
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_inc_denied') SETTINGS base_backup=$bk FORMAT Null" 2>&1 \
    | grep -c -m1 'READ ON FILE'
echo "-- with READ ON FILE as well: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO $user"
deny_or_allow "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_inc_ok') SETTINGS base_backup=$bk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ, WRITE ON FILE FROM $user"

echo "-- the source grant a BACKUP consumed is accounted to the BACKUP query"
# `BackupStarter` runs the check on a copy of the query context, whose `QueryPrivilegesInfo` is
# reset by `makeQueryContext`, so the grant reaches system.query_log only while that tracker is
# shared with the originating query.
audit_qid="04774_audit_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query_id "$audit_qid" -q \
    "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_audit') FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
SELECT has(used_privileges, 'WRITE ON FILE')
FROM system.query_log
WHERE query_id = '$audit_qid' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time_microseconds DESC LIMIT 1"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON FILE FROM $user"

echo "-- an admin holding SOURCES is unaffected"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE $src.t TO File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b_adm') FORMAT Null"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE $src.t AS $db.r_adm FROM $bk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT x FROM $db.r_adm"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP DATABASE IF EXISTS ${db}_b1;
DROP DATABASE IF EXISTS ${db}_b2;
DROP DATABASE IF EXISTS ${db}_b4;
DROP DATABASE IF EXISTS $src;
DROP USER IF EXISTS $user;
"
