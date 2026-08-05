#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a compact column-ID part whose columns.txt is reordered relative to the
# physical substream layout must be REJECTED by CHECK -- full-column reads seek
# by columns.txt ordinal, so a reorder silently reads swapped columns.  A
# legitimate RENAME must still PASS (the check resolves by column ID, not name).
#
# `min_bytes_for_full_part_storage = 0` forces Full part storage: this test edits
# `columns.txt` as a standalone on-disk file, which does not exist under Packed
# storage (all metadata lives inside one blob).  Without this, a randomized
# `min_bytes_for_full_part_storage` makes the tiny part Packed and the file edit fails.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

# Scenario 1: a legitimately renamed compact column-ID part still passes CHECK.
$CLIENT --query "DROP TABLE IF EXISTS t_rename_ok SYNC"
$CLIENT --query "
CREATE TABLE t_rename_ok (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 1000000000,
         min_rows_for_wide_part = 1000000000,
         min_bytes_for_full_part_storage = 0;
"
echo "INSERT INTO t_rename_ok VALUES (1, 'x', 1.5), (2, 'y', 2.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_rename_ok RENAME COLUMN b TO b2"
echo "rename check: $($CLIENT --query "CHECK TABLE t_rename_ok SETTINGS check_query_single_value_result = 1")"
$CLIENT --query "DROP TABLE t_rename_ok SYNC"

# Scenario 2: a reordered columns.txt is rejected.
$CLIENT --query "DROP TABLE IF EXISTS t_reorder SYNC"
$CLIENT --query "
CREATE TABLE t_reorder (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 1000000000,
         min_rows_for_wide_part = 1000000000,
         min_bytes_for_full_part_storage = 0;
"
echo "INSERT INTO t_reorder VALUES (1, 'x', 1.5), (2, 'y', 2.5)" | $CLIENT

part_path=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_reorder' AND active LIMIT 1")
$CLIENT --query "DETACH TABLE t_reorder SYNC"

# Swap the `b` and `c` entries in columns.txt while leaving columns_substreams.txt
# (the physical layout) untouched.
python3 - "${part_path}columns.txt" <<'PY'
import sys
path = sys.argv[1]
with open(path) as f:
    lines = f.readlines()
# lines[0]: "columns format version: N", lines[1]: "N columns:", then one line per column.
body = lines[2:]
assert len(body) == 3, body
body[1], body[2] = body[2], body[1]
with open(path, 'w') as f:
    f.writelines(lines[:2] + body)
PY

attach_out=$($CLIENT --query "ATTACH TABLE t_reorder" 2>&1 || true)
if [ -n "${attach_out}" ]; then
    echo "reorder: rejected"
else
    check_out=$($CLIENT --query "CHECK TABLE t_reorder SETTINGS check_query_single_value_result = 1" 2>&1 || true)
    if [ "${check_out}" = "1" ]; then
        echo "reorder: NOT rejected"
    else
        echo "reorder: rejected"
    fi
fi

$CLIENT --query "DROP TABLE IF EXISTS t_reorder SYNC"
