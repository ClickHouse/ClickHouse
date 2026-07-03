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

# Second phase: a Dynamic column with no default. This is a distinct facet of the
# same race. A merge applies the pending rename on-fly, but the source parts still
# physically store the old name; the merge sees the new name in metadata, finds it
# absent from the parts, and (because a Dynamic column has no default) expires and
# drops it from the merged part, so every value reads back as NULL. The String
# column above is saved by its default; the Dynamic column is not.
for i in $(seq 1 "$ITERATIONS"); do
    $CLICKHOUSE_CLIENT --query="
        DROP TABLE IF EXISTS t_rename_merge_race_dynamic;

        SET allow_experimental_dynamic_type = 1;

        CREATE TABLE t_rename_merge_race_dynamic (x UInt64, y UInt64)
        ENGINE = MergeTree() ORDER BY x
        SETTINGS min_bytes_for_wide_part = 0;

        INSERT INTO t_rename_merge_race_dynamic SELECT number, number FROM numbers(3);
        ALTER TABLE t_rename_merge_race_dynamic ADD COLUMN d Dynamic SETTINGS mutations_sync = 1;
        INSERT INTO t_rename_merge_race_dynamic SELECT number, number, number FROM numbers(3, 3);
        INSERT INTO t_rename_merge_race_dynamic SELECT number, number, 'str_' || toString(number) FROM numbers(6, 3);
        INSERT INTO t_rename_merge_race_dynamic SELECT number, number, NULL FROM numbers(9, 3);
        INSERT INTO t_rename_merge_race_dynamic SELECT number, number, multiIf(number % 3 = 0, number, number % 3 = 1, 'str_' || toString(number), NULL) FROM numbers(12, 3);
    "

    $CLICKHOUSE_CLIENT --query="ALTER TABLE t_rename_merge_race_dynamic RENAME COLUMN d TO d1 SETTINGS alter_sync = 2" &
    pid_alter=$!
    $CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE t_rename_merge_race_dynamic FINAL" &
    pid_optimize=$!

    set +e
    wait "$pid_alter"
    alter_status=$?
    wait "$pid_optimize"
    optimize_status=$?
    set -e

    if [ "$alter_status" -ne 0 ] || [ "$optimize_status" -ne 0 ]; then
        echo "FAIL on dynamic iteration $i: alter exited with $alter_status, optimize exited with $optimize_status"
        $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race_dynamic"
        exit 1
    fi

    # 8 of the 15 rows hold a non-null Dynamic value. Before the fix they read
    # back as NULL, dropping this count to 0.
    count=$($CLICKHOUSE_CLIENT --query="SELECT count() FROM t_rename_merge_race_dynamic WHERE d1 IS NOT NULL SETTINGS allow_experimental_dynamic_type = 1")
    if [ "$count" != "8" ]; then
        echo "FAIL on dynamic iteration $i: expected 8 non-null Dynamic rows, got $count"
        $CLICKHOUSE_CLIENT --query="SELECT x, d1 FROM t_rename_merge_race_dynamic ORDER BY x SETTINGS allow_experimental_dynamic_type = 1"
        $CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race_dynamic"
        exit 1
    fi
done

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race"
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_rename_merge_race_dynamic"
echo "OK"
