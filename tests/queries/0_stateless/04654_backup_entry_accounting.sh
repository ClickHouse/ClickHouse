#!/usr/bin/env bash
# Tags: no-fasttest, no-ordinary-database, no-encrypted-storage
# ^ needs the 'backups' disk; the KeeperMap renames need table UUIDs; encrypted disks make no two files identical.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# num_entries / uncompressed_size count the objects a backup physically stores, so the BACKUP and RESTORE
# rows of one backup must agree, and the count must not depend on object naming. data_file_name_generator=
# 'checksum' names an object after its content, which is where a name-based count breaks.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Three identical columns in a wide part give several byte-identical files, stored as one object each.
# min_bytes_for_full_part_storage=0 keeps the part one file per column: packed part storage would put the
# whole part in a single data.packed file, leaving nothing to deduplicate.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt64, b UInt64, c UInt64) ENGINE=MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0;
INSERT INTO t SELECT number, number, number FROM numbers(1000);
"

roundtrip() {
    local tag=$1 backup_settings=$2 table=${3:-t}
    ${CLICKHOUSE_CLIENT} --query "
        BACKUP TABLE ${table} TO Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_backup'${backup_settings}" | grep -o BACKUP_CREATED
    ${CLICKHOUSE_CLIENT} --query "
        RESTORE TABLE ${table} AS ${table}_${tag} FROM Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_restore'" | grep -o RESTORED
}

roundtrip first ""
roundtrip checksum ", data_file_name_generator='checksum'"
# A plain backup stores every file as its own object, duplicates included.
roundtrip nodedup ", deduplicate_files=0"

# With every array empty, the Log table's element stream is an empty file, which a plain backup stores too.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS empty_stream;
CREATE TABLE empty_stream (s Array(UInt64)) ENGINE=Log;
INSERT INTO empty_stream SELECT [] FROM numbers(10);
"
roundtrip nodedup_empty ", deduplicate_files=0" empty_stream

# Two KeeperMap tables sharing one Keeper path store the data once, so the second's entry references the first's file.
# The reference has no object of its own: with packing on, it must not be counted in addition to the pack holding the bytes.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE km_a (key UInt64, value String) ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
CREATE TABLE km_b (key UInt64, value String) ENGINE=KeeperMap('/' || currentDatabase() || '/04654') PRIMARY KEY key;
INSERT INTO km_a SELECT number, 'v' || toString(number) FROM numbers(50);
"

# The miscount appears only if the reference is iterated before its target, so back the pair up under both name orders.
packref_backup() {
    local tag=$1 name_a=$2 name_b=$3
    ${CLICKHOUSE_CLIENT} --query "RENAME TABLE km_a TO ${name_a}, km_b TO ${name_b}"
    ${CLICKHOUSE_CLIENT} --query "
        BACKUP TABLE ${name_a}, TABLE ${name_b} TO Disk('backups', '${name}_${tag}')
        SETTINGS id='${name}_${tag}_backup', backup_use_packed_format=1" | grep -o BACKUP_CREATED
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

for tag in first checksum nodedup nodedup_empty packref packref_swapped; do
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
DROP TABLE empty_stream; DROP TABLE empty_stream_nodedup_empty;
DROP TABLE km_a SYNC; DROP TABLE km_b SYNC"
