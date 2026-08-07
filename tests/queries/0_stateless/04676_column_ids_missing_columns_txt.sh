#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a wide part with no columns.txt rebuilds its column list from table metadata, already
# stamped with IDs, so resolving names from the ID a second time would leave every column keyed by
# its NAME while the files are named by ID -- a post-activation column then reads a wrong stream.
#
# `min_bytes_for_full_part_storage = 0` keeps columns.txt a standalone file, which this test
# deletes; under Packed storage it lives inside one blob.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

$CLIENT --query "DROP TABLE IF EXISTS t_no_columns_txt SYNC"
$CLIENT --query "
CREATE TABLE t_no_columns_txt (a UInt32, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0;
"
echo "INSERT INTO t_no_columns_txt VALUES (1, 'x'), (2, 'y')" | $CLIENT

# A column added after activation gets a numeric ID, so its stream files are named `1.bin`, not `c.bin`.
$CLIENT --query "ALTER TABLE t_no_columns_txt ADD COLUMN c String DEFAULT 'z'"
$CLIENT --query "ALTER TABLE t_no_columns_txt UPDATE c = 'w' WHERE 1"

part_path=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_no_columns_txt' AND active LIMIT 1")
$CLIENT --query "DETACH TABLE t_no_columns_txt SYNC"
rm "${part_path}columns.txt"
$CLIENT --query "ATTACH TABLE t_no_columns_txt"

$CLIENT --query "SELECT * FROM t_no_columns_txt ORDER BY a"
echo "rewritten columns.txt tokens: $(tail -n +3 "${part_path}columns.txt" | cut -d' ' -f1 | paste -sd' ' -)"
echo "check: $($CLIENT --query "CHECK TABLE t_no_columns_txt SETTINGS check_query_single_value_result = 1")"

$CLIENT --query "DROP TABLE IF EXISTS t_no_columns_txt SYNC"
