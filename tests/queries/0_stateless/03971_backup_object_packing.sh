#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan
# ^ backups need a running server with a configured 'backups' disk; too slow under sanitizer overhead +
#   the flaky check's 8x concurrency (trips the 180s per-test cap). Non-sanitizer builds run it in seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# experimental_backup_pack_format bundles many small backup blobs into a few pack objects. Row counts are
# tiny on purpose: the number of data files, hence of packs, follows the schema and not the row count, so a
# few dozen rows exercises every path.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
backups_disk_path="$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'")"

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
small_insert="INSERT INTO t SELECT number, toString(number) FROM numbers(50)"

# Case 1: small files only -> packed.
roundtrip "small-only" "Disk('backups', '${name}_1')" "$small_create" "$small_insert" \
    "SETTINGS experimental_backup_pack_format=1"

# Case 2: mixed small + big; big blobs stay their own object, small ones packed.
roundtrip "mixed" "Disk('backups', '${name}_2')" \
    "CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0" \
    "INSERT INTO t SELECT number, repeat('x', 200) FROM numbers(50)" \
    "SETTINGS experimental_backup_pack_format=1, backup_pack_min_size=200"

# Case 3: small files spilling across a few packs. pack_size is chosen just under the packable total so it
# spills into 2-3 packs (kept tiny on purpose) -- then assert >1 pack actually formed.
roundtrip "multi-pack" "Disk('backups', '${name}_3')" "$small_create" \
    "$small_insert" \
    "SETTINGS experimental_backup_pack_format=1, backup_pack_size=800"
npacks=$(ls "${backups_disk_path}${name}_3"/packs_* 2>/dev/null | wc -l)
if [ "$npacks" -gt 1 ]; then echo "multi-pack spilled"; else echo "multi-pack ONLY $npacks pack(s)"; fi

# Case 4: duplicated data (two identical columns) -> members collapse via dedup.
roundtrip "dedup" "Disk('backups', '${name}_4')" \
    "CREATE TABLE t (a UInt64, b UInt64) ENGINE=MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=0" \
    "INSERT INTO t SELECT number, number FROM numbers(50)" \
    "SETTINGS experimental_backup_pack_format=1"
# A duplicate of a packed member must not write a loose own-object (it would land nested under data/...),
# so the backup dir holds nothing but the manifest and the packs.
find "${backups_disk_path}${name}_4" -type f ! -name '.backup' ! -name 'packs_*' | wc -l

# Case 6: setting off (default) still works -> regression guard.
roundtrip "setting-off" "Disk('backups', '${name}_6')" "$small_create" "$small_insert" ""

# Case 5: incremental backup on top of a packed base.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_base') SETTINGS experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number, toString(number) FROM numbers(50, 50)"
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
python3 - "${backups_disk_path}${name}_9.zip" <<'PY'
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

# Case 10: packing is unsupported on the plain path (deduplicate_files=0), which doesn't build the dedup
# identity packing needs -- reject rather than silently no-op.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_10') SETTINGS experimental_backup_pack_format=1, deduplicate_files=0" 2>&1 \
    | grep -o "not supported with deduplicate_files" | head -1

# Case 11: the manifest's <packed> markers and the packs' front indexes state the same membership, and
# restore must reject any disagreement -- a member dropped from an index would otherwise be read as an own
# object of the same data_file id. Each case tampers with a copy of one good packed backup.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_11') SETTINGS experimental_backup_pack_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

restore_tampered() {
    local mode=$1
    local dir="${backups_disk_path}${name}_11_${mode}"
    rm -rf "$dir"
    cp -r "${backups_disk_path}${name}_11" "$dir"
    python3 - "$dir" "$mode" <<'PY'
import glob, os, re, struct, sys

directory, mode = sys.argv[1], sys.argv[2]
manifest_path = os.path.join(directory, ".backup")

if mode != "drop_index_member":
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = f.read()
    if mode == "drop_num_packs":
        manifest = re.sub(r"<num_packs>\d+</num_packs>", "", manifest, count=1)
    elif mode == "downgrade_version":
        manifest = re.sub(r"<version>\d+</version>", "<version>1</version>", manifest, count=1)
    else:
        manifest = manifest.replace("<packed>true</packed>", "")
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(manifest)
    sys.exit(0)

pack_path = sorted(glob.glob(os.path.join(directory, "packs_*")))[0]
with open(pack_path, "rb") as f:
    pack = f.read()

def read_varint(pos):
    value = shift = 0
    while True:
        byte = pack[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7

def write_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)

version = pack[0]
entry_format = "<QQQ" if version >= 1 else "<QQ"
entry_size = struct.calcsize(entry_format)
(count,) = struct.unpack_from("<Q", pack, 1)
pos = 9
members = []
for _ in range(count):
    length, pos = read_varint(pos)
    name = pack[pos:pos + length]
    pos += length
    members.append((name, struct.unpack_from(entry_format, pack, pos)))
    pos += entry_size

# Drop the first member. Bodies keep their absolute offsets, and the shortened index is zero-padded back to
# its original length, so every surviving member still resolves -- only the dropped one goes missing.
index = bytearray(pack[:1]) + struct.pack("<Q", len(members) - 1)
for name, entry in members[1:]:
    index += write_varint(len(name)) + name + struct.pack(entry_format, *entry)
index += b"\0" * (pos - len(index))
with open(pack_path, "wb") as f:
    f.write(bytes(index) + pack[pos:])
PY
    ${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_11_${mode}')" 2>&1 \
        | grep -oE "marked as packed in the metadata but no pack contains it|is not marked as packed in the metadata|predates the packed format" | head -1
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
}

restore_tampered drop_index_member
restore_tampered drop_num_packs
restore_tampered drop_markers
restore_tampered downgrade_version

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
