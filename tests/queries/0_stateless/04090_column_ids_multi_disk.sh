#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: the two places a multi-disk layout changes column-ID handling.
#   1. FREEZE: a shadow is an immutable, portable, PER-DISK artifact; offline tools
#      read one disk's subtree at a time, so `column_ids.json` must land in the
#      shadow of EACH disk holding frozen parts, not just one.
#   2. ATTACH: with the authoritative copy absent, the legacy multi-disk fallback
#      must fail closed when the remaining copies diverge -- adopting one silently
#      could cement a stale mapping (resurrecting DROP+re-ADD bytes under a reused
#      name).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

server_path=$($CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'path'")
server_path="${server_path%/}"

resolve() { case "$1" in /*) echo "$1" ;; *) echo "${server_path}/$1" ;; esac; }

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

$CLIENT --query "ALTER TABLE t_freeze_disks FREEZE WITH NAME '${backup_name}'"

n_maps=$(find "${server_path}" -path "*/shadow/${backup_name}/*" -name column_ids.json 2>/dev/null | wc -l | tr -d ' ')
echo "freeze mapping copies: ${n_maps}"

$CLIENT --query "ALTER TABLE t_freeze_disks UNFREEZE WITH NAME '${backup_name}'" > /dev/null
$CLIENT --query "DROP TABLE t_freeze_disks SYNC"

# 2. ATTACH refuses divergent legacy per-disk copies. Destructive (plants files on
#    disk), so it runs last and on its own table.

$CLIENT --query "DROP TABLE IF EXISTS t_divergent SYNC"
create_table t_divergent
echo "INSERT INTO t_divergent VALUES (1, 'x')" | $CLIENT

# data_paths[1] is the authoritative (policy-first) disk; [2],[3] are others.
auth=$(resolve "$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_divergent'")")
other2=$(resolve "$($CLIENT --query "SELECT data_paths[2] FROM system.tables WHERE database = currentDatabase() AND name = 't_divergent'")")
other3=$(resolve "$($CLIENT --query "SELECT data_paths[3] FROM system.tables WHERE database = currentDatabase() AND name = 't_divergent'")")

$CLIENT --query "DETACH TABLE t_divergent SYNC"

# Drop the authoritative copy and plant two DIVERGENT copies on other disks
# (the legacy multi-disk layout after a torn write / partial cleanup).
rm -f "${auth}column_ids.json"
printf '%s' '{"active": true, "next_column_id": 3, "mapping": {"a": "a", "b": "b"}}' > "${other2}column_ids.json"
printf '%s' '{"active": true, "next_column_id": 8, "mapping": {"a": "a", "b": "7"}}' > "${other3}column_ids.json"

attach_out=$($CLIENT --query "ATTACH TABLE t_divergent" 2>&1 || true)
echo "${attach_out}" | grep -q "differs between legacy disk copies" && echo "rejected: divergent copies" || echo "NOT rejected"

$CLIENT --query "DROP TABLE IF EXISTS t_divergent SYNC" 2>/dev/null || true
