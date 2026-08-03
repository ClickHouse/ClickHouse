#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: an INACTIVE leftover column_ids.json must not make ALTER treat the table
# as column-ID-active. Parts still use logical filenames, so RENAME must take
# the normal (mutation) path that renames files on disk, not the metadata-only
# path -- otherwise reads of the renamed column break.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1 --mutations_sync=2"

$CLIENT --query "DROP TABLE IF EXISTS t_inactive_leftover SYNC"

# Legacy table (no column IDs): parts are written with logical filenames.
$CLIENT --query "
CREATE TABLE t_inactive_leftover (a UInt32, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
"

echo "INSERT INTO t_inactive_leftover VALUES (1, 'hello'), (2, 'world')" | $CLIENT

table_dir=$($CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 't_inactive_leftover'")
mapping_file="${table_dir}column_ids.json"

$CLIENT --query "DETACH TABLE t_inactive_leftover SYNC"

# Plant an INACTIVE, empty leftover mapping -- the exact artifact an old
# failed-activation used to write. It loads as a non-null but inactive mapping.
printf '%s' '{"active": false, "next_column_id": 1, "mapping": {}}' > "${mapping_file}"

$CLIENT --query "ATTACH TABLE t_inactive_leftover"

$CLIENT --query "ALTER TABLE t_inactive_leftover RENAME COLUMN b TO b2"

echo "select after rename:"
$CLIENT --query "SELECT a, b2 FROM t_inactive_leftover ORDER BY a"

$CLIENT --query "DROP TABLE t_inactive_leftover SYNC"
