#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a plain-counter column ID derives its offsets stream from the LOGICAL Nested parent, a dotted
# one from its ID parent. A group must therefore keep ONE convention: a new child added beside
# plain-ID siblings has to be plain too, or the group ends up with two offsets streams. The plain
# shape is only reachable through a mapping an older build wrote, so it is planted by file surgery.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

# Plants @2 as the table's mapping, which only a load reads.
plant_mapping()
{
    local table_dir
    table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '$1'")
    $CLIENT --query "DETACH TABLE $1 SYNC"
    printf '%s' "$2" > "${table_dir}column_ids.json"
    $CLIENT --query "ATTACH TABLE $1"
}

offsets_streams_query()
{
    echo "
    SELECT column, column_id, arrayFilter(f -> position(f, 'size0') > 0, filenames)
    FROM system.parts_columns
    WHERE database = currentDatabase() AND table = '$1' AND active AND column LIKE 'n.%'
    ORDER BY column"
}

# Section 1: a new child in an all-plain group stays plain and shares the group's one offsets stream.
$CLIENT --query "DROP TABLE IF EXISTS t_legacy_nested SYNC"
$CLIENT --query "
CREATE TABLE t_legacy_nested (k UInt64, \`n.x\` Array(UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         serialization_info_version = 'with_column_ids';
"
plant_mapping t_legacy_nested '{"active": true, "next_column_id": 2, "mapping": {"k": "k", "n.x": "1"}}'

$CLIENT --query "ALTER TABLE t_legacy_nested ADD COLUMN \`n.y\` Array(String)"
$CLIENT --query "INSERT INTO t_legacy_nested VALUES (1, [10, 20, 30], ['zz', 'yy', 'xx'])" </dev/null

echo "plain group, ids and offsets streams:"
$CLIENT --query "$(offsets_streams_query t_legacy_nested)"

echo "plain group, cross-parent rename:"
$CLIENT --query "ALTER TABLE t_legacy_nested RENAME COLUMN \`n.y\` TO \`m.y\`" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

$CLIENT --query "DROP TABLE t_legacy_nested SYNC"

# Section 2: the residual an older build could already have persisted -- one plain child and one
# dotted child in the same group, so the two children sit on different offsets streams. Moving the
# dotted child out has to be rejected: the plain sibling's stream is tied to the logical parent.
$CLIENT --query "DROP TABLE IF EXISTS t_mixed_nested SYNC"
$CLIENT --query "
CREATE TABLE t_mixed_nested (k UInt64, \`n.x\` Array(UInt64), \`n.y\` Array(String))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         serialization_info_version = 'with_column_ids';
"
plant_mapping t_mixed_nested '{"active": true, "next_column_id": 3, "mapping": {"k": "k", "n.x": "1", "n.y": "2.y"}}'

$CLIENT --query "INSERT INTO t_mixed_nested VALUES (1, [10, 20], ['a', 'b'])" </dev/null

echo "mixed group, ids and offsets streams:"
$CLIENT --query "$(offsets_streams_query t_mixed_nested)"

echo "mixed group, cross-parent rename of the dotted child:"
$CLIENT --query "ALTER TABLE t_mixed_nested RENAME COLUMN \`n.y\` TO \`m.y\`" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

$CLIENT --query "DROP TABLE t_mixed_nested SYNC"
