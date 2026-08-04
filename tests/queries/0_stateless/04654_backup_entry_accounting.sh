#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-encrypted-storage
# ^ backups need a running server with a configured 'backups' disk; the KeeperMap case below needs table UUIDs,
#   without which renaming a table can move the data to the other one.
#   encrypted disks add a random per-file IV, so no two files are byte-identical and nothing deduplicates.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# num_entries / uncompressed_size count the objects a backup physically stores, so the BACKUP and RESTORE
# rows of one backup must agree, and the count must not depend on object naming. data_file_name_generator=
# 'checksum' names an object after its content, which is where a name-based count breaks.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Three identical columns in a wide part give several byte-identical files, stored as one object each.
# min_bytes_for_full_part_storage=0 keeps the part one file per column: packed part storage would put the
# whole part in a single data.packed blob, leaving nothing to deduplicate.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64, b UInt64, c UInt64) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0;
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
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE km_a (key UInt64, value String) ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
CREATE TABLE km_b (key UInt64, value String) ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
INSERT INTO km_a SELECT number, 'v' || toString(number) FROM numbers(50);
"

# The miscount only shows when the reference is iterated before its target, and which table holds the data is
# not ours to pick. Iteration follows the backup path order, which the names decide, so back the same pair up
# under both name orders; taking both backups before either restore keeps the roles fixed between them.
packref_backup() {
    local tag=$1 name_a=$2 name_b=$3
    ${CLICKHOUSE_CLIENT} --query "RENAME TABLE km_a TO ${name_a}, km_b TO ${name_b}"
    ${CLICKHOUSE_CLIENT} --query "
        BACKUP TABLE ${name_a}, TABLE ${name_b} TO Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_backup', experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
    ${CLICKHOUSE_CLIENT} --query "RENAME TABLE ${name_a} TO km_a, ${name_b} TO km_b"
}

packref_restore() {
    local tag=$1 name_a=$2 name_b=$3
    ${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS km_a SYNC; DROP TABLE IF EXISTS km_b SYNC"
    ${CLICKHOUSE_CLIENT} --query "
        RESTORE TABLE ${name_a}, TABLE ${name_b} FROM Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_restore'" | grep -o RESTORED
    ${CLICKHOUSE_CLIENT} --query "RENAME TABLE ${name_a} TO km_a, ${name_b} TO km_b"
}

packref_backup packref aaa_km zzz_km
packref_backup packref_swapped zzz_km aaa_km
packref_restore packref aaa_km zzz_km
packref_restore packref_swapped zzz_km aaa_km

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS backup_log"

for tag in first checksum nodedup packref packref_swapped; do
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
DROP TABLE km_a SYNC; DROP TABLE km_b SYNC"
