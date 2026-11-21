#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel -- enables a server-wide failpoint that affects empty-part creation for all tables.
# no-shared-merge-tree -- the covering-empty-part path being tested is for plain MergeTree.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="create_empty_part_inject_stale_dir"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_empty_part_stale_tmp SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_empty_part_stale_tmp SYNC"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_empty_part_stale_tmp (a Int32) ENGINE = MergeTree() PARTITION BY intDiv(a, 20) ORDER BY a;
"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_empty_part_stale_tmp SELECT number FROM numbers(60)"

# DROP PARTITION covers the dropped parts with a new empty part. The failpoint makes
# createEmptyPart() find a pre-existing temporary directory for that empty part, simulating a
# stale leftover from a previously interrupted DROP/DETACH/MOVE/REPLACE PARTITION (the part was
# promoted to PreActive with a deferred rename and then rolled back, leaving tmp_empty_<part> on
# disk). createEmptyPart() must reclaim the stale directory instead of aborting the server with
# LOGICAL_ERROR "New empty part is about to materialize but the directory already exist".
#
# Enable, drop and disable the failpoint in a single client invocation so the server-wide
# failpoint is armed only for this one DROP PARTITION. send_logs_level=error hides the expected
# "Removing old temporary directory" warning emitted while reclaiming the stale directory.
$CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
SYSTEM ENABLE FAILPOINT $FP;
ALTER TABLE t_empty_part_stale_tmp DROP PARTITION 2;
SYSTEM DISABLE FAILPOINT $FP;
"

# The partition is gone and the server is healthy.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_empty_part_stale_tmp WHERE intDiv(a, 20) = 2"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_empty_part_stale_tmp"
