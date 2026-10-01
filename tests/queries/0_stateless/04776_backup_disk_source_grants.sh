#!/usr/bin/env bash
# Tags: no-encrypted-storage, no-replicated-database
# no-replicated-database: the CREATE DATABASE runs with no user there, so its check is a no-op.

# A `Disk(...)` backup location is authorized against the `SOURCES` grant model like every other
# backup engine: writing needs `WRITE ON DISK` and reading needs `READ ON DISK`. `DISK` is its own
# source, so a `FILE` grant does not reach a disk and a `DISK` grant does not reach a path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04776_${CLICKHOUSE_DATABASE}"
src="${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
bk="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/b')"
bk_disk="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk')"

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
# Admin-made backups the restricted user will try to read. Both locators are needed: the cross-arm
# below reads the File one while holding only the DISK grant.
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $src TO $bk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $src TO $bk_disk FORMAT Null"

deny_or_allow() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "allowed"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then echo "denied"
    else echo "unexpected: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"; fi
}

echo "-- BACKUP TO Disk(...) without WRITE ON DISK: denied, and it names the missing grant"
${CLICKHOUSE_CLIENT} --user "$user" -q \
    "BACKUP TABLE $src.t TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk2') FORMAT Null" 2>&1 \
    | grep -c -m1 'WRITE ON DISK'
echo "-- BACKUP TO Disk(...) with WRITE ON DISK: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON DISK TO $user"
deny_or_allow "BACKUP TABLE $src.t TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_disk3') FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON DISK FROM $user"

echo "-- RESTORE FROM Disk(...) without READ ON DISK: denied"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd1 FROM $bk_disk FORMAT Null"
echo "-- the table was not created by the rejected restore"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '$db' AND name = 'rd1'"
echo "-- RESTORE FROM Disk(...) with READ ON DISK: allowed and the row is readable"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd2 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM $db.rd2"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON DISK FROM $user"

echo "-- WRITE ON DISK alone does not authorize reading a backup"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON DISK TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd3 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON DISK FROM $user"

echo "-- FILE and DISK are separate capabilities: neither grant authorizes the other locator"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd4 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON FILE FROM $user"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd5 FROM $bk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON DISK FROM $user"

echo "-- CREATE DATABASE ENGINE = Backup over Disk(...) without READ ON DISK: denied"
deny_or_allow "CREATE DATABASE ${db}_b1 ENGINE = Backup('$src', $bk_disk)"
echo "-- the database was not created"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${db}_b1'"
echo "-- CREATE DATABASE ENGINE = Backup over Disk(...) with READ ON DISK: allowed and readable"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO $user"
deny_or_allow "CREATE DATABASE ${db}_b2 ENGINE = Backup('$src', $bk_disk)"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM ${db}_b2.t"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON DISK FROM $user"

echo "-- SOURCES covers the new source, so an admin-level grant needs no update"
${CLICKHOUSE_CLIENT} -q "GRANT SOURCES ON *.* TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd6 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE SOURCES ON *.* FROM $user"

echo "-- the source name also accepts the Disk spelling the locator itself uses"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON Disk TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.rd7 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON DISK FROM $user"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP DATABASE IF EXISTS ${db}_b1;
DROP DATABASE IF EXISTS ${db}_b2;
DROP DATABASE IF EXISTS $src;
DROP USER IF EXISTS $user;
"
