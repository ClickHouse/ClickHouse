#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: columns.txt spells IDs, so CHECK compares it to the list the part loaded from it in ID
# space.  A renamed column must pass; a columns.txt that no longer agrees with the loaded part
# must be rejected, exactly as without column IDs.
#
# `min_bytes_for_full_part_storage = 0` keeps columns.txt a standalone file (Packed storage puts
# it inside one blob).  Wide part, so the compact substream-slot check cannot be what fires.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

$CLIENT --query "DROP TABLE IF EXISTS t_columns_txt SYNC"
$CLIENT --query "
CREATE TABLE t_columns_txt (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;
"
echo "INSERT INTO t_columns_txt VALUES (1, 'x', 1.5), (2, 'y', 2.5)" | $CLIENT

# A metadata-only rename leaves the write-time ID in columns.txt: names differ from the file,
# IDs do not.
$CLIENT --query "ALTER TABLE t_columns_txt RENAME COLUMN b TO b2"
echo "after rename: $($CLIENT --query "CHECK TABLE t_columns_txt SETTINGS check_query_single_value_result = 1")"

part_path=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_columns_txt' AND active LIMIT 1")

# Swap two entries of the on-disk columns.txt while the part stays loaded, so the file no
# longer describes the column list the part holds.  Types differ, so a reader following the
# file's order would decode Float64 bytes as a String.
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

check_out=$($CLIENT --query "CHECK TABLE t_columns_txt SETTINGS check_query_single_value_result = 1" 2>&1 || true)
if [ "${check_out}" = "1" ]; then
    echo "edited columns.txt: NOT rejected"
else
    echo "edited columns.txt: rejected"
fi

$CLIENT --query "DROP TABLE IF EXISTS t_columns_txt SYNC"
