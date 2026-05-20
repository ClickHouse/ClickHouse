#!/usr/bin/env bash
# Tags: long
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

# 50 attempts keeps total wall time under the per-test timeout on slow sanitizer
# builds (TSan ~20x slower than Debug, MSan/ASan+UBSan ~3-5x) while still
# catching the regression with very high probability: at the observed 2-6%
# per-attempt race rate on the buggy master, a single run misses the bug with
# probability between ~5% (at the 6% high end) and ~36% (at the 2% low end) —
# so any single sanitizer/build variant catches the regression 64-95% of the
# time, and the 4+ build variants we run combined drive the miss probability
# into the noise.
ITERATIONS=50

for i in $(seq 1 "$ITERATIONS"); do
    # Batch DROP/CREATE/INSERTs into a single client invocation. Most of the
    # per-iteration wall time on sanitizer builds is `clickhouse-client`
    # process startup, so collapsing 7 separate invocations into 1 saves a
    # large fraction of the per-iteration cost without changing semantics —
    # each `INSERT` statement still creates its own part.
    $CLICKHOUSE_CLIENT --query="
        DROP TABLE IF EXISTS t_rename_merge_race;

        CREATE TABLE t_rename_merge_race (id UInt64, d String DEFAULT '')
        ENGINE = MergeTree() ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0;

        -- Create 5 separate parts to give OPTIMIZE FINAL something to merge.
        INSERT INTO t_rename_merge_race VALUES (1, 'hello'), (2, 'world');
        INSERT INTO t_rename_merge_race VALUES (3, 'foo'), (4, 'bar');
        INSERT INTO t_rename_merge_race VALUES (5, 'baz'), (6, 'qux');
        INSERT INTO t_rename_merge_race VALUES (7, 'alpha'), (8, 'beta');
        INSERT INTO t_rename_merge_race VALUES (9, 'gamma'), (10, 'delta');
    "

    # Run the ALTER RENAME and OPTIMIZE FINAL concurrently. Use alter_sync=2 to
    # wait for the rename mutation to fully apply before we read the result.
    $CLICKHOUSE_CLIENT --query="ALTER TABLE t_rename_merge_race RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" &
    pid_alter=$!
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_rename_merge_race FINAL" &
    pid_optimize=$!

    # Wait for both processes independently and capture each exit status.
    # `wait $pid1 $pid2` returns only the LAST PID's status in Bash, which would
    # silently mask an `ALTER` failure when `OPTIMIZE` succeeded (or vice versa)
    # and weaken the regression signal. Use `set +e` so we can record both
    # statuses before deciding whether to fail the iteration.
    set +e
    wait "$pid_alter"
    alter_status=$?
    wait "$pid_optimize"
    optimize_status=$?
    set -e

    if [ "$alter_status" -ne 0 ] || [ "$optimize_status" -ne 0 ]; then
        echo "FAIL on iteration $i: alter exited with $alter_status, optimize exited with $optimize_status"
        $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race"
        exit 1
    fi

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
