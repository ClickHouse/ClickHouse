#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/80648

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Each iteration spawns 4 clickhouse-client processes; under sanitizers + the
# parallel runner that startup cost dominates, so the iteration count is the
# main lever on wall time. At the ~20% per-attempt data-loss rate on buggy
# master, 25 attempts catch the regression with >99% probability per build.
ITERATIONS=25

for i in $(seq 1 "$ITERATIONS"); do
    # One client invocation: each INSERT still makes its own part.
    $CLICKHOUSE_CLIENT --query="
        DROP TABLE IF EXISTS t_rename_merge_race;

        CREATE TABLE t_rename_merge_race (id UInt64, d String DEFAULT '')
        ENGINE = MergeTree() ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0;

        INSERT INTO t_rename_merge_race VALUES (1, 'hello'), (2, 'world');
        INSERT INTO t_rename_merge_race VALUES (3, 'foo'), (4, 'bar');
        INSERT INTO t_rename_merge_race VALUES (5, 'baz'), (6, 'qux');
        INSERT INTO t_rename_merge_race VALUES (7, 'alpha'), (8, 'beta');
        INSERT INTO t_rename_merge_race VALUES (9, 'gamma'), (10, 'delta');
    "

    # alter_sync=2 waits for the rename mutation to fully apply.
    $CLICKHOUSE_CLIENT --query="ALTER TABLE t_rename_merge_race RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" &
    pid_alter=$!
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_rename_merge_race FINAL" &
    pid_optimize=$!

    # `wait pid1 pid2` returns only the last status, masking a failure of the
    # other process; wait on each separately so both statuses are checked.
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

    # Before the fix the merge fills d1 with empty defaults, dropping this below 10.
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
