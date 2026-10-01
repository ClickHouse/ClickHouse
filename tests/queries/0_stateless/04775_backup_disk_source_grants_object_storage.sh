#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage, no-replicated-database
# no-fasttest: disk_s3_plain_rewritable_03517 is only installed when EXPORT_S3_STORAGE_POLICIES=1.
# no-replicated-database: kept from 04776 as the conservative choice; no arm here is known to need
# it. Not re-verified: that job's 3-replica cluster and Keeper ensemble need more than one server.

# A `Disk(...)` backup location requires READ/WRITE ON DISK whatever the disk is backed by. This
# guards the object-storage case, which 04776 cannot cover because its disk is local.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
user="user_04775_${CLICKHOUSE_DATABASE}"
src="${CLICKHOUSE_TEST_UNIQUE_NAME}_src"
# A plain directory, not an archive: zip archives are rejected on object-storage-backed disks.
bk_disk="Disk('disk_s3_plain_rewritable_03517', '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3')"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS $src"
${CLICKHOUSE_CLIENT} --multiquery -q "
CREATE DATABASE $src;
CREATE TABLE $src.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $src.t VALUES (42);
CREATE USER $user;
GRANT BACKUP ON $src.* TO $user;
GRANT CREATE TABLE, INSERT, SELECT ON $db.* TO $user;
GRANT SELECT ON $src.* TO $user;
-- Needed under table_engines_require_grant. It confers no SOURCES, so every denial below still
-- comes from SOURCES.
GRANT TABLE ENGINE ON *.* TO $user;
REVOKE SOURCES ON *.* FROM $user;
"
# An admin-made backup the restricted user will try to read.
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE $src TO $bk_disk FORMAT Null"

deny_or_allow() {
    local out
    out=$(${CLICKHOUSE_CLIENT} --user "$user" -q "$1" 2>&1)
    if [ $? -eq 0 ]; then echo "allowed"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then echo "denied"
    else echo "unexpected: $(echo "$out" | grep -oE 'Code: [0-9]+' | head -1)"; fi
}

echo "-- RESTORE from an S3-backed disk without READ ON DISK: denied"
deny_or_allow "RESTORE TABLE $src.t AS $db.r1 FROM $bk_disk FORMAT Null"
echo "-- and it names the missing grant"
${CLICKHOUSE_CLIENT} --user "$user" -q "RESTORE TABLE $src.t AS $db.r1 FROM $bk_disk FORMAT Null" 2>&1 \
    | grep -c -m1 'READ ON DISK'

echo "-- READ ON S3 does not authorize it: the requirement is the locator, not the backing store"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON S3 TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.r2 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON S3 FROM $user"

echo "-- with READ ON DISK: allowed and the row is readable"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO $user"
deny_or_allow "RESTORE TABLE $src.t AS $db.r3 FROM $bk_disk FORMAT Null"
${CLICKHOUSE_CLIENT} --user "$user" -q "SELECT x FROM $db.r3"
${CLICKHOUSE_CLIENT} -q "REVOKE READ ON DISK FROM $user"

echo "-- BACKUP to an S3-backed disk without WRITE ON DISK: denied"
deny_or_allow "BACKUP TABLE $src.t TO Disk('disk_s3_plain_rewritable_03517', '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3b') FORMAT Null"
echo "-- with WRITE ON DISK: allowed"
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON DISK TO $user"
deny_or_allow "BACKUP TABLE $src.t TO Disk('disk_s3_plain_rewritable_03517', '${CLICKHOUSE_TEST_UNIQUE_NAME}_s3c') FORMAT Null"
${CLICKHOUSE_CLIENT} -q "REVOKE WRITE ON DISK FROM $user"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP DATABASE IF EXISTS $src;
DROP USER IF EXISTS $user;
"
