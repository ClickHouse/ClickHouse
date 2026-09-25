#!/usr/bin/env bash
# Tags: long

# Cancelling a background Vertical merge that reads in a separate thread must not rethrow the exception
# of the reading thread from `cancel`. When both threads see the cancellation, the merge thread may throw
# before it pulls the exception of the reading thread, which then stays in the executor.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_read_thread_cancel;
    CREATE TABLE t_read_thread_cancel (id UInt64, s1 String, s2 String, s3 String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
        enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
        vertical_merge_read_in_separate_thread = 1, min_age_to_force_merge_seconds = 1;
    SYSTEM STOP MERGES t_read_thread_cancel;
"
for i in {0..3}; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO t_read_thread_cancel
        SELECT number, repeat(toString(number), 20), repeat(toString(number), 20), repeat(toString(number), 20)
        FROM numbers($((i * 500000)), 500000)"
done

# Each iteration starts merges and cancels them while they read columns.
for _ in {1..30}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t_read_thread_cancel"
    sleep 0.$((RANDOM % 5 + 1))
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t_read_thread_cancel"
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM START MERGES t_read_thread_cancel;
    OPTIMIZE TABLE t_read_thread_cancel FINAL;
    SELECT count(), sum(id), countIf(s1 = repeat(toString(id), 20)) FROM t_read_thread_cancel;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_read_thread_cancel' AND active;
    DROP TABLE t_read_thread_cancel;
"
