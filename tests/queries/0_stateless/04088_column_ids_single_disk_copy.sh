#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: `column_ids.json` is the live name<->id oracle only (parts are
# self-describing), so it lives as a SINGLE authoritative copy on one disk.
# On a multi-disk policy exactly one copy must exist, and the table must load
# and survive a reload after an ALTER that rewrites the mapping.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

$CLIENT --query "DROP TABLE IF EXISTS t_single_copy SYNC"

# policy_02961 is a local multi-disk policy (disk1..disk4_02961).
$CLIENT --query "
CREATE TABLE t_single_copy (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS storage_policy = 'policy_02961',
         serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
"

for i in 1 2 3 4 5 6; do
    echo "INSERT INTO t_single_copy VALUES (${i}, 'v${i}', ${i}.5)" | $CLIENT
done

# The policy's disks are configured with relative paths, so system.tables
# reports paths relative to the server's data directory; resolve them.
server_path=$($CLIENT --query "SELECT value FROM system.server_settings WHERE name = 'path'")
server_path="${server_path%/}"

count_copies() {
    local n=0
    while read -r p; do
        [ -z "$p" ] && continue
        case "$p" in
            /*) full="$p" ;;
            *)  full="${server_path}/$p" ;;
        esac
        [ -f "${full}column_ids.json" ] && n=$((n + 1))
    done < <($CLIENT --query "SELECT arrayJoin(data_paths) FROM system.tables WHERE database = currentDatabase() AND name = 't_single_copy'")
    echo "$n"
}

echo "copies after create+insert: $(count_copies)"

$CLIENT --query "DETACH TABLE t_single_copy SYNC"
$CLIENT --query "ATTACH TABLE t_single_copy"
echo "rows after reload: $($CLIENT --query 'SELECT count() FROM t_single_copy')"

# RENAME rewrites the mapping; the single-copy invariant must hold afterwards.
$CLIENT --query "ALTER TABLE t_single_copy RENAME COLUMN b TO b2"
echo "copies after rename: $(count_copies)"

$CLIENT --query "DETACH TABLE t_single_copy SYNC"
$CLIENT --query "ATTACH TABLE t_single_copy"
echo "select after rename+reload:"
$CLIENT --query "SELECT a, b2, c FROM t_single_copy ORDER BY a"

$CLIENT --query "DROP TABLE t_single_copy SYNC"
