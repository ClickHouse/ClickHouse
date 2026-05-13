#!/usr/bin/env bash
# Tags: long, no-parallel
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80648
#
# Reproduces a race between `ALTER TABLE ... RENAME COLUMN` and a concurrent
# `OPTIMIZE TABLE ... FINAL` in `StorageMergeTree`. Before the fix, a merge
# scheduled in the window between `setProperties` (which publishes the new
# in-memory metadata with the renamed column) and `startMutation` (which
# registers the rename mutation in `current_mutations_by_version`) would read
# old parts using the new column name, find no on-disk file, and fill the
# new column with default values — losing the renamed column's data.
#
# The original reproducer triggers the race in 2-6 of 100 attempts, so we
# loop many iterations and fail on the first lost row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# 100 attempts is enough to catch the bug with very high probability on master:
# the original analysis observed 2-6% per attempt, so the chance of NOT seeing
# any data loss in 100 attempts is below 1e-6 in the buggy case.
ITERATIONS=100

for i in $(seq 1 "$ITERATIONS"); do
    $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race"

    $CLICKHOUSE_CLIENT --query="
        CREATE TABLE t_rename_merge_race (id UInt64, d String DEFAULT '')
        ENGINE = MergeTree() ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0"

    # Create 5 separate parts to give OPTIMIZE FINAL something to merge.
    $CLICKHOUSE_CLIENT --query="INSERT INTO t_rename_merge_race VALUES (1, 'hello'), (2, 'world')"
    $CLICKHOUSE_CLIENT --query="INSERT INTO t_rename_merge_race VALUES (3, 'foo'), (4, 'bar')"
    $CLICKHOUSE_CLIENT --query="INSERT INTO t_rename_merge_race VALUES (5, 'baz'), (6, 'qux')"
    $CLICKHOUSE_CLIENT --query="INSERT INTO t_rename_merge_race VALUES (7, 'alpha'), (8, 'beta')"
    $CLICKHOUSE_CLIENT --query="INSERT INTO t_rename_merge_race VALUES (9, 'gamma'), (10, 'delta')"

    # Run the ALTER RENAME and OPTIMIZE FINAL concurrently. Use alter_sync=2 to
    # wait for the rename mutation to fully apply before we read the result.
    $CLICKHOUSE_CLIENT --query="ALTER TABLE t_rename_merge_race RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" &
    pid_alter=$!
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_rename_merge_race FINAL" &
    pid_optimize=$!

    wait "$pid_alter" "$pid_optimize"

    # Count rows where the renamed column still has its original (non-empty) value.
    # Before the fix, the merge would replace `d` data with empty defaults, so
    # this count would drop below 10.
    count=$($CLICKHOUSE_CLIENT --query="SELECT count() FROM t_rename_merge_race WHERE d1 != ''")
    if [ "$count" != "10" ]; then
        echo "FAIL on iteration $i: expected 10 non-empty rows, got $count"
        $CLICKHOUSE_CLIENT --query="SELECT id, d1 FROM t_rename_merge_race ORDER BY id"
        $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race"
echo "OK"
