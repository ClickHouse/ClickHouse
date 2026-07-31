#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# num_entries / uncompressed_size count the objects a backup physically stores, so the BACKUP row and the
# RESTORE row of one backup must report the same numbers, and the count must not depend on how the objects
# were named. data_file_name_generator='checksum' names an object after its content, which is where a
# name-based count breaks: every byte-identical file matches the name on write and none matches it on read.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Three identical columns in a wide part give several byte-identical files, stored as one object each.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64, b UInt64, c UInt64) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0;
INSERT INTO t SELECT number, number, number FROM numbers(1000);
"

roundtrip() {
    local tag=$1 backup_settings=$2
    ${CLICKHOUSE_CLIENT} --query "
        BACKUP TABLE t TO Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_backup'${backup_settings}" | grep -o BACKUP_CREATED
    ${CLICKHOUSE_CLIENT} --query "
        RESTORE TABLE t AS t_${tag} FROM Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_restore'" | grep -o RESTORED
}

roundtrip first ""
roundtrip checksum ", data_file_name_generator='checksum'"
roundtrip nodedup ", deduplicate_files=0"

# Two KeeperMap tables sharing one Keeper path store the data once: the second table's entry is a reference to
# the first table's file. A reference inherits its target's object name but keeps pack_id == -1, so with
# packing on it must not be counted as an object of its own -- the pack already accounts for those bytes.
# The explicit UUIDs pin which table holds the data (BackupCoordinationKeeperMapTables keeps the largest table
# id) so the reference always sorts before its target, the order in which a per-file pack test miscounts.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE zzz_target UUID 'ffffffff-ffff-4fff-8fff-ffffffffffff' (key UInt64, value String)
    ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
CREATE TABLE aaa_reference UUID '00000000-0000-4000-8000-000000000001' (key UInt64, value String)
    ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
INSERT INTO zzz_target SELECT number, 'v' || toString(number) FROM numbers(50);
"
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE aaa_reference, TABLE zzz_target TO Disk('backups', '${name}_packref')
    SETTINGS id='${name}_packref_backup', experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE aaa_reference SYNC; DROP TABLE zzz_target SYNC"
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE aaa_reference, TABLE zzz_target FROM Disk('backups', '${name}_packref')
    SETTINGS id='${name}_packref_restore'" | grep -o RESTORED

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS backup_log"

for tag in first checksum nodedup packref; do
    ${CLICKHOUSE_CLIENT} --query "
    SELECT '$tag',
        (SELECT (num_entries, uncompressed_size) FROM system.backup_log
            WHERE id='${name}_${tag}_backup' AND status='BACKUP_CREATED')
        = (SELECT (num_entries, uncompressed_size) FROM system.backup_log
            WHERE id='${name}_${tag}_restore' AND status='RESTORED') AS backup_and_restore_agree"
done

${CLICKHOUSE_CLIENT} --query "
SELECT
    (SELECT num_entries FROM system.backup_log WHERE id='${name}_first_backup' AND status='BACKUP_CREATED')
    = (SELECT num_entries FROM system.backup_log WHERE id='${name}_checksum_backup' AND status='BACKUP_CREATED')
        AS naming_keeps_object_count,
    (SELECT num_entries < num_files FROM system.backup_log
        WHERE id='${name}_first_backup' AND status='BACKUP_CREATED') AS duplicates_collapsed,
    (SELECT num_entries = num_files FROM system.backup_log
        WHERE id='${name}_nodedup_backup' AND status='BACKUP_CREATED') AS every_file_is_an_object"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE t; DROP TABLE t_first; DROP TABLE t_checksum; DROP TABLE t_nodedup;
DROP TABLE aaa_reference SYNC; DROP TABLE zzz_target SYNC"
