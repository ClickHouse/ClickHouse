#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a FREEZE shadow is an immutable, portable, PER-DISK artifact; offline
# tools read one disk's subtree at a time, so `column_ids.json` must be written
# into the shadow of EACH disk that holds frozen parts, not just one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

server_path=$($CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'path'")
server_path="${server_path%/}"
backup_name="cid_freeze_${CLICKHOUSE_DATABASE}"

$CLIENT --query "DROP TABLE IF EXISTS t_freeze_disks SYNC"
$CLIENT --query "
CREATE TABLE t_freeze_disks (a UInt32, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS storage_policy = 'policy_02961',
         serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"
# Keep the two parts separate on their disks (no merge consolidating them).
$CLIENT --query "SYSTEM STOP MERGES t_freeze_disks"
echo "INSERT INTO t_freeze_disks VALUES (1, 'x')" | $CLIENT
echo "INSERT INTO t_freeze_disks VALUES (2, 'y')" | $CLIENT

# Spread the two parts across two distinct disks.
$CLIENT --query "ALTER TABLE t_freeze_disks MOVE PART 'all_1_1_0' TO DISK 'disk1_02961'"
$CLIENT --query "ALTER TABLE t_freeze_disks MOVE PART 'all_2_2_0' TO DISK 'disk2_02961'"
echo "distinct part disks: $($CLIENT --query "SELECT countDistinct(disk_name) FROM system.parts WHERE database = currentDatabase() AND table = 't_freeze_disks' AND active")"

$CLIENT --query "ALTER TABLE t_freeze_disks FREEZE WITH NAME '${backup_name}'"

# Each frozen disk's shadow subtree must carry its own column_ids.json.
n_maps=$(find "${server_path}" -path "*/shadow/${backup_name}/*" -name column_ids.json 2>/dev/null | wc -l | tr -d ' ')
echo "freeze mapping copies: ${n_maps}"

$CLIENT --query "ALTER TABLE t_freeze_disks UNFREEZE WITH NAME '${backup_name}'" > /dev/null
$CLIENT --query "DROP TABLE t_freeze_disks SYNC"
