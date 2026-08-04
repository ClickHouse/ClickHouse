#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# B34: BACKUP / RESTORE of a cas table in an Atomic (UUID) database — the default in
# the stateless suite. Two behaviors:
#   1. BACKUP uses the pointer-holding path (make_temporary_hard_links=false): it resolves the
#      part's objects via getStorageObjects and never calls disk->createHardLink, so it succeeds on
#      a cas disk. (The temporary-hard-link BACKUP path, used only by the deprecated
#      Ordinary database engine, is fail-closed with a clear SUPPORT_IS_DISABLED message in
#      DataPartStorageOnDiskBase::backup — see B34/B16.)
#   2. RESTORE onto a cas disk now succeeds end-to-end: the part files are written back
#      through the disk's write path and the restored table reads back identical to the original.
#      (This used to fail closed with NOT_IMPLEMENTED until the whole-part write contract, B30,
#      landed; restore-onto-CA is no longer an M1 gap.)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}.zip')"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS t_cas_backup;
DROP TABLE IF EXISTS t_cas_restored;

CREATE TABLE t_cas_backup (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '04284',
    name = '04284_cas_backup',
    path = '04284_cas_backup_pool/');

INSERT INTO t_cas_backup SELECT number, toString(number % 7) FROM numbers(1000);
SELECT 'before', count(), sum(a), uniqExact(s) FROM t_cas_backup;
EOF

# (1) Pointer-holding BACKUP of the Atomic-DB cas table succeeds.
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE t_cas_backup TO ${backup_name}" | cut -f2

# (2) RESTORE onto a cas disk succeeds and the restored data is identical.
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE t_cas_backup AS t_cas_restored FROM ${backup_name}" | cut -f2
${CLICKHOUSE_CLIENT} -q "SELECT 'after', count(), sum(a), uniqExact(s) FROM t_cas_restored"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS t_cas_restored;
DROP TABLE t_cas_backup;
SELECT 'dropped_ok';
EOF
