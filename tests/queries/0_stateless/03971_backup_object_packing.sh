#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# experimental_backup_pack_format bundles many small backup blobs into a few pack objects (the
# PackedFilesIO ".packed" format). Every case below must restore byte-identical data.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

checksum() { ${CLICKHOUSE_CLIENT} --query "SELECT sum(cityHash64(*)) FROM $1"; }

roundtrip() {
    local label=$1 backup=$2 create=$3 insert=$4 backup_settings=$5
    ${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $create"
    ${CLICKHOUSE_CLIENT} --query "$insert"
    local before after
    before=$(checksum t)
    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO $backup $backup_settings" | grep -o BACKUP_CREATED
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
    ${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM $backup" | grep -o RESTORED
    after=$(checksum t)
    if [ "$before" = "$after" ]; then echo "$label identical"; else echo "$label MISMATCH $before != $after"; fi
}

small_create="CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0"
small_insert="INSERT INTO t SELECT number, toString(number) FROM numbers(2000)"

# Case 1: small files only -> packed.
roundtrip "small-only" "Disk('backups', '${name}_1')" "$small_create" "$small_insert" \
    "SETTINGS experimental_backup_pack_format=1"

# Case 2: mixed small + big; big blobs stay their own object, small ones packed.
roundtrip "mixed" "Disk('backups', '${name}_2')" \
    "CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0" \
    "INSERT INTO t SELECT number, repeat('x', 200) FROM numbers(5000)" \
    "SETTINGS experimental_backup_pack_format=1, backup_pack_min_size=200"

# Case 3: many small files spilling across several packs.
roundtrip "multi-pack" "Disk('backups', '${name}_3')" "$small_create" \
    "INSERT INTO t SELECT number, toString(number) FROM numbers(3000)" \
    "SETTINGS experimental_backup_pack_format=1, backup_pack_size=256"

# Case 4: duplicated data (two identical columns) -> members collapse via dedup.
roundtrip "dedup" "Disk('backups', '${name}_4')" \
    "CREATE TABLE t (a UInt64, b UInt64) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0" \
    "INSERT INTO t SELECT number, number FROM numbers(4000)" \
    "SETTINGS experimental_backup_pack_format=1"

# Case 6: setting off (default) still works -> regression guard.
roundtrip "setting-off" "Disk('backups', '${name}_6')" "$small_create" "$small_insert" ""

# Case 5: incremental backup on top of a packed base.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_base') SETTINGS experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number, toString(number) FROM numbers(2000, 2000)"
incr_before=$(checksum t)
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_incr') SETTINGS experimental_backup_pack_format=1, base_backup=Disk('backups', '${name}_base')" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_incr') SETTINGS base_backup=Disk('backups', '${name}_base')" | grep -o RESTORED
incr_after=$(checksum t)
if [ "$incr_before" = "$incr_after" ]; then echo "incremental identical"; else echo "incremental MISMATCH"; fi

# Case 7: packing is mutually exclusive with an archive destination.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_7.zip') SETTINGS experimental_backup_pack_format=1" 2>&1 \
    | grep -o "is not supported with an archive destination" | head -1

# Case 8: system.backups accounting. A pack is one entry whose stored size includes the serialized front index.
# Back up the same data unpacked and packed: packing collapses many small member objects into few pack objects
# (strictly fewer entries) and the pack's index bytes make its stored size larger than the raw member payload.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_8off') SETTINGS id='${name}_8off'" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_8on') SETTINGS id='${name}_8on', experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "
SELECT
    (SELECT num_entries FROM system.backups WHERE id='${name}_8on') <
    (SELECT num_entries FROM system.backups WHERE id='${name}_8off') AS packed_has_fewer_entries,
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_8on') >
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_8off') AS packed_size_includes_index"

# The RESTORE (read-side) row must report the same num_entries and uncompressed_size as the BACKUP (write-side)
# row for a packed backup -- the read side accounts packs the same way, not per member.
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_8on') SETTINGS id='${name}_8restore'" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "
SELECT
    (SELECT num_entries FROM system.backups WHERE id='${name}_8restore') =
    (SELECT num_entries FROM system.backups WHERE id='${name}_8on') AS restore_entries_match_backup,
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_8restore') =
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_8on') AS restore_size_matches_backup"

# Case 9: a tampered archive manifest carrying <num_packs> must fail closed on read. An archive is never packed
# on write (rejected), so num_packs>0 in an archive backup is corruption -- RESTORE must reject it as
# BACKUP_DAMAGED instead of chasing a non-existent sibling packs_* object through the plain reader.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_9.zip')" | grep -o BACKUP_CREATED
archive_path="$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'")/${name}_9.zip"
python3 - "$archive_path" <<'PY'
import sys, zipfile, os
path = sys.argv[1]
tmp = path + ".tmp"
with zipfile.ZipFile(path, "r") as zin, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename == ".backup":
            data = data.decode("utf-8").replace("<contents>", "<num_packs>1</num_packs><contents>", 1).encode("utf-8")
        zout.writestr(item, data)
os.replace(tmp, path)
PY
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_9.zip')" 2>&1 \
    | grep -o "Archive backup cannot contain packed objects" | head -1

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
