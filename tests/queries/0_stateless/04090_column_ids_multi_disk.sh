#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: the two places a multi-disk layout changes column-ID handling.
#   1. FREEZE: a shadow is an immutable, portable, PER-DISK artifact; offline tools
#      read one disk's subtree at a time, so `column_ids.json` must land in the
#      shadow of EACH disk holding frozen parts, not just one. The live table is the
#      opposite: one copy, on the policy's first disk, before and after an ALTER
#      rewrites it.
#   2. ATTACH: `column_ids.json` is read from the policy's first disk and nowhere else,
#      so a copy on another disk must not rescue the table -- nothing at load time can
#      prove it current, and a stale one resurrects DROP+re-ADD bytes under a reused name.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

server_path=$($CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'path'")
server_path="${server_path%/}"

resolve() { case "$1" in /*) echo "$1" ;; *) echo "${server_path}/$1" ;; esac; }

# How many of the table's disks hold a `column_ids.json`.
count_copies() {
    local n=0
    while read -r p; do
        [ -n "$p" ] && [ -f "$(resolve "$p")column_ids.json" ] && n=$((n + 1))
    done < <($CLIENT --query "SELECT arrayJoin(data_paths) FROM system.tables WHERE database = currentDatabase() AND name = '$1'")
    echo "$n"
}

# Both sections need a multi-disk table with column IDs active and wide parts.
create_table() {
    $CLIENT --query "
    CREATE TABLE $1 (a UInt32, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS storage_policy = 'policy_02961',
             serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    "
}

# 1. FREEZE writes the mapping into every frozen disk's shadow subtree.

backup_name="cid_freeze_${CLICKHOUSE_DATABASE}"

$CLIENT --query "DROP TABLE IF EXISTS t_freeze_disks SYNC"
create_table t_freeze_disks

# Keep the two parts separate on their disks (no merge consolidating them).
$CLIENT --query "SYSTEM STOP MERGES t_freeze_disks"
echo "INSERT INTO t_freeze_disks VALUES (1, 'x')" | $CLIENT
echo "INSERT INTO t_freeze_disks VALUES (2, 'y')" | $CLIENT

$CLIENT --query "ALTER TABLE t_freeze_disks MOVE PART 'all_1_1_0' TO DISK 'disk1_02961'"
$CLIENT --query "ALTER TABLE t_freeze_disks MOVE PART 'all_2_2_0' TO DISK 'disk2_02961'"
echo "distinct part disks: $($CLIENT --query "SELECT countDistinct(disk_name) FROM system.parts WHERE database = currentDatabase() AND table = 't_freeze_disks' AND active")"

echo "live mapping copies: $(count_copies t_freeze_disks)"

$CLIENT --query "ALTER TABLE t_freeze_disks FREEZE WITH NAME '${backup_name}'"

n_maps=$(find "${server_path}" -path "*/shadow/${backup_name}/*" -name column_ids.json 2>/dev/null | wc -l | tr -d ' ')
echo "freeze mapping copies: ${n_maps}"

$CLIENT --query "ALTER TABLE t_freeze_disks UNFREEZE WITH NAME '${backup_name}'" > /dev/null

# A RENAME rewrites the mapping: still one copy, and the reloaded table reads through it.
$CLIENT --query "ALTER TABLE t_freeze_disks RENAME COLUMN b TO b2"
echo "live mapping copies after rename: $(count_copies t_freeze_disks)"
$CLIENT --query "DETACH TABLE t_freeze_disks SYNC"
$CLIENT --query "ATTACH TABLE t_freeze_disks"
$CLIENT --query "SELECT a, b2 FROM t_freeze_disks ORDER BY a"

$CLIENT --query "DROP TABLE t_freeze_disks SYNC"

# 2. ATTACH refuses to adopt a copy that is not on the authoritative disk. Destructive
#    (moves files on disk), so it runs last and on its own table.

$CLIENT --query "DROP TABLE IF EXISTS t_off_disk SYNC"
create_table t_off_disk
echo "INSERT INTO t_off_disk VALUES (1, 'x')" | $CLIENT

# DROP + re-ADD moves `b` to the numeric ID "1", so the closing SELECT can only return
# the right values through the right mapping.
$CLIENT --query "ALTER TABLE t_off_disk DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_off_disk ADD COLUMN b String"
echo "INSERT INTO t_off_disk VALUES (2, 'y')" | $CLIENT

# data_paths[1] is the authoritative (policy-first) disk; [2] is another one.
auth=$(resolve "$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_off_disk'")")
other=$(resolve "$($CLIENT --query "SELECT data_paths[2] FROM system.tables WHERE database = currentDatabase() AND name = 't_off_disk'")")

$CLIENT --query "DETACH TABLE t_off_disk SYNC"

# Exactly ONE copy is the point: a lone copy has nothing to disagree with, so comparing
# copies against each other cannot catch it -- only refusing outright can.  This is the
# shape a skipped cleanup leaves behind on a read-only disk, or a removal that threw.
mkdir -p "${other}"
mv "${auth}column_ids.json" "${other}column_ids.json"

attach_out=$($CLIENT --query "ATTACH TABLE t_off_disk" 2>&1 || true)
echo "${attach_out}" | grep -q "column_ids.json" && echo "rejected: mapping off the authoritative disk" || echo "NOT rejected"

# Discriminators that do not depend on stderr text: the table must not be loaded, and
# the refusal must not have migrated a copy back onto the authoritative disk.
echo "loaded after refusal: $($CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_off_disk'")"
[ -f "${auth}column_ids.json" ] && echo "copy migrated back" || echo "copy not adopted"

# Recover the way the error message tells the operator to, and check the data survived.
# Conditional because a server that wrongly adopted the copy has already moved it back.
if [ -f "${other}column_ids.json" ]; then
    mv "${other}column_ids.json" "${auth}column_ids.json"
fi
$CLIENT --query "ATTACH TABLE t_off_disk" 2>/dev/null || true
$CLIENT --query "SELECT a, b FROM t_off_disk ORDER BY a"

$CLIENT --query "DROP TABLE IF EXISTS t_off_disk SYNC" 2>/dev/null || true

# 3. Section 2 reads the mapping from the policy's first disk and nowhere else, so an ALTER that moves
#    that disk would strand it. Both settings decide it -- `disk` wins over `storage_policy` -- and a
#    RESET shows up only in the recomputed settings: `RESET SETTING disk` sends the table back to the
#    default policy without `changeSettingsImpl` ever seeing a `disk` entry to compatibility-check.
#    Unrefused, it left a table that could not ATTACH.

$CLIENT --query "DROP TABLE IF EXISTS t_move_disk SYNC"
$CLIENT --query "
    CREATE TABLE t_move_disk (a UInt32, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS disk = 'disk1_02961',
             serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0;
"
echo "INSERT INTO t_move_disk VALUES (1, 'x')" | $CLIENT

echo "reset: $($CLIENT --query "ALTER TABLE t_move_disk RESET SETTING disk" 2>&1 | grep -o 'SUPPORT_IS_DISABLED' | head -1)"
echo "modify: $($CLIENT --query "ALTER TABLE t_move_disk MODIFY SETTING disk = 'disk2_02961'" 2>&1 | grep -o 'SUPPORT_IS_DISABLED' | head -1)"

# Neither ALTER landed, so the mapping is still on the disk the table reads it from.
$CLIENT --query "ALTER TABLE t_move_disk RENAME COLUMN b TO d"
$CLIENT --query "DETACH TABLE t_move_disk SYNC"
$CLIENT --query "ATTACH TABLE t_move_disk"
$CLIENT --query "SELECT a, d FROM t_move_disk ORDER BY a"

$CLIENT --query "DROP TABLE IF EXISTS t_move_disk SYNC"
