#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# why: when the two-phase prune fails to persist, the retained old name stays in the mapping for the
# rest of the server's lifetime -- only a table load prunes it. An ADD COLUMN of that name in the
# same lifetime must get a FRESH column ID: reusing the retained entry's ID would bind the new
# column to another column's on-disk stream, or resurrect a dropped column's data.

# The failed prune and the retained-entry drop are logged on purpose, and the client forwards server
# logs to its stderr, which the harness reads as a failure.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_before_mapping_prune" 2>/dev/null
}
trap cleanup EXIT

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

# Section 1: a DROP whose prune did not persist. Nothing else is live under the retained name, so no
# pre-check fires: reusing the retained ID would resurrect the dropped column's data and recycle
# an ID that old parts still carry.
$CLIENT --query "DROP TABLE IF EXISTS t_readd_drop SYNC"
$CLIENT --query "
CREATE TABLE t_readd_drop (k UInt64, a String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         serialization_info_version = 'with_column_ids';
"
$CLIENT --query "INSERT INTO t_readd_drop VALUES (1, 'dropped_data')" </dev/null

$CLIENT --query "SYSTEM ENABLE FAILPOINT column_ids_throw_before_mapping_prune"
$CLIENT --query "ALTER TABLE t_readd_drop DROP COLUMN a"
$CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_before_mapping_prune"

$CLIENT --query "ALTER TABLE t_readd_drop ADD COLUMN a String"
$CLIENT --query "INSERT INTO t_readd_drop (k, a) VALUES (2, 'fresh')" </dev/null

echo "drop: re-added a does not read the dropped column's data:"
$CLIENT --query "SELECT k, a FROM t_readd_drop ORDER BY k"
$CLIENT --query "OPTIMIZE TABLE t_readd_drop FINAL"
echo "drop: after merge:"
$CLIENT --query "SELECT k, a FROM t_readd_drop ORDER BY k"
echo "drop: fresh id for a:"
$CLIENT --query "
SELECT DISTINCT column_id FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_drop' AND active AND column = 'a'"

$CLIENT --query "DETACH TABLE t_readd_drop SYNC"
$CLIENT --query "ATTACH TABLE t_readd_drop"
echo "drop: reattached:"
$CLIENT --query "SELECT k, a FROM t_readd_drop ORDER BY k"

$CLIENT --query "DROP TABLE t_readd_drop SYNC"

# Section 2: rename residue outliving the renamed column. Dropping `b` leaves the retained `a` entry
# as the only claim on the ID, so the re-added `a` would silently read the pre-rename bytes.
$CLIENT --query "DROP TABLE IF EXISTS t_readd_chain SYNC"
$CLIENT --query "
CREATE TABLE t_readd_chain (k UInt64, a String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         serialization_info_version = 'with_column_ids';
"
$CLIENT --query "INSERT INTO t_readd_chain VALUES (1, 'orig')" </dev/null

$CLIENT --query "SYSTEM ENABLE FAILPOINT column_ids_throw_before_mapping_prune"
$CLIENT --query "ALTER TABLE t_readd_chain RENAME COLUMN a TO b"
$CLIENT --query "SYSTEM DISABLE FAILPOINT column_ids_throw_before_mapping_prune"

$CLIENT --query "ALTER TABLE t_readd_chain DROP COLUMN b"
$CLIENT --query "ALTER TABLE t_readd_chain ADD COLUMN a String"
$CLIENT --query "INSERT INTO t_readd_chain (k, a) VALUES (2, 'fresh')" </dev/null

echo "chain: re-added a does not read the pre-rename data:"
$CLIENT --query "SELECT k, a FROM t_readd_chain ORDER BY k"
$CLIENT --query "OPTIMIZE TABLE t_readd_chain FINAL"
echo "chain: fresh id for a:"
$CLIENT --query "
SELECT DISTINCT column_id FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_chain' AND active AND column = 'a'"

$CLIENT --query "DETACH TABLE t_readd_chain SYNC"
$CLIENT --query "ATTACH TABLE t_readd_chain"
echo "chain: reattached:"
$CLIENT --query "SELECT k, a FROM t_readd_chain ORDER BY k"

$CLIENT --query "DROP TABLE t_readd_chain SYNC"
