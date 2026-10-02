#!/usr/bin/env bash
# A .backup manifest whose <version> fits in UInt64 but not in int must be rejected, not silently
# narrowed past the supported-version check (e.g. 4294967298 must not wrap to 2 and be accepted).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_ver_overflow"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE tbl_ver_overflow (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl_ver_overflow VALUES (1)"

backups_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'" 2>/dev/null)
if [ -z "${backups_disk_root}" ]; then
    echo "backups disk is not configured, skipping test"
    exit 0
fi

bname="${CLICKHOUSE_TEST_UNIQUE_NAME}_ver"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl_ver_overflow TO Disk('backups', '${bname}')" > /dev/null 2>&1

# 4294967298 = 2^32 + 2: fits in UInt64 but narrows to 2 as int, which would pass the range check.
sed -i "s|<version>[0-9]*</version>|<version>4294967298</version>|" "${backups_disk_root}/${bname}/.backup"

${CLICKHOUSE_CLIENT} --query "DROP TABLE tbl_ver_overflow"
${CLICKHOUSE_CLIENT} -m -q "RESTORE TABLE tbl_ver_overflow FROM Disk('backups', '${bname}'); -- { serverError BACKUP_VERSION_NOT_SUPPORTED }"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS tbl_ver_overflow"
rm -rf "${backups_disk_root:?}/${bname}" 2>/dev/null || true
