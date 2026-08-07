#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a compact part records its substreams per columns.txt slot, under the write-time column ID.
# A metadata-only RENAME changes neither, so CHECK must still pass -- it resolves by ID, not by the
# column's current name.  Wide's twin is 04675.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

$CLIENT --query "DROP TABLE IF EXISTS t_rename_ok SYNC"
$CLIENT --query "
CREATE TABLE t_rename_ok (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 1000000000,
         min_rows_for_wide_part = 1000000000;
"
echo "INSERT INTO t_rename_ok VALUES (1, 'x', 1.5), (2, 'y', 2.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_rename_ok RENAME COLUMN b TO b2"
echo "rename check: $($CLIENT --query "CHECK TABLE t_rename_ok SETTINGS check_query_single_value_result = 1")"
$CLIENT --query "SELECT * FROM t_rename_ok ORDER BY a"
$CLIENT --query "DROP TABLE t_rename_ok SYNC"
