#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# The test enables a server-wide failpoint and exercises the plain `MergeTree` insert barrier.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP=mt_insert_pause_before_commit_part
INSERT_PID=

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}" 2>/dev/null ||:
    if [[ -n "${INSERT_PID}" ]]; then
        wait "${INSERT_PID}" 2>/dev/null ||:
    fi
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_empty_index_guard; DROP TABLE IF EXISTS t_empty_materialized_guard; DROP TABLE IF EXISTS t_race_materialized; DROP TABLE IF EXISTS t_race_index; DROP TABLE IF EXISTS t_race_index_drop" 2>/dev/null ||:
}
trap cleanup EXIT

# Even while empty, a matcher change is a non-metadata `ALTER` because a write using the old
# metadata may still be in flight.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_empty_index_guard (a UInt64, y UInt64 ALIAS greatest(a, * EXCEPT y), INDEX idx y TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT --allow_non_metadata_alters 0 -q "ALTER TABLE t_empty_index_guard ADD COLUMN b UInt64 DEFAULT a + 1000" 2>&1 \
    | grep -o 'ALTER_OF_COLUMN_IS_FORBIDDEN' | head -1
$CLICKHOUSE_CLIENT -q "DROP TABLE t_empty_index_guard"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_empty_materialized_guard (a UInt64, m UInt64 MATERIALIZED greatest(a, * EXCEPT m)) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT --allow_non_metadata_alters 0 -q "ALTER TABLE t_empty_materialized_guard ADD COLUMN b UInt64 DEFAULT a + 1000" 2>&1 \
    | grep -o 'ALTER_OF_COLUMN_IS_FORBIDDEN' | head -1
$CLICKHOUSE_CLIENT -q "DROP TABLE t_empty_materialized_guard"

# Pause an `INSERT` after it has written a temporary part with the old `MATERIALIZED` expression but
# before it allocates the part block number. The `ALTER` must wait for the `INSERT` share lock instead
# of registering an earlier mutation version that the late part would skip.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_race_materialized (a UInt64, m UInt64 MATERIALIZED greatest(a, * EXCEPT m)) ENGINE = MergeTree ORDER BY a"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_race_materialized (a) VALUES (1)" > "${CLICKHOUSE_TMP}/05024_materialized_insert.log" 2>&1 &
INSERT_PID=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT --lock_acquire_timeout 1 -q "ALTER TABLE t_race_materialized ADD COLUMN b UInt64 DEFAULT a + 1000" 2>&1 \
    | grep -o 'DEADLOCK_AVOIDED' | head -1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait "${INSERT_PID}"
INSERT_PID=
$CLICKHOUSE_CLIENT --alter_sync 2 -q "ALTER TABLE t_race_materialized ADD COLUMN b UInt64 DEFAULT a + 1000"
$CLICKHOUSE_CLIENT -q "SELECT a, b, m FROM t_race_materialized"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_race_materialized"

# Repeat with an index file built from the old `ALIAS` expansion. After the barrier drains the old
# `INSERT`, the queued mutation must rebuild that file before the forced-index query runs.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_race_index (a UInt64, y UInt64 ALIAS greatest(a, * EXCEPT y), INDEX idx y TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_race_index (a) VALUES (1)" > "${CLICKHOUSE_TMP}/05024_index_insert.log" 2>&1 &
INSERT_PID=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT --lock_acquire_timeout 1 -q "ALTER TABLE t_race_index ADD COLUMN b UInt64 DEFAULT a + 1000" 2>&1 \
    | grep -o 'DEADLOCK_AVOIDED' | head -1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait "${INSERT_PID}"
INSERT_PID=
$CLICKHOUSE_CLIENT --alter_sync 2 -q "ALTER TABLE t_race_index ADD COLUMN b UInt64 DEFAULT a + 1000"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_race_index WHERE y = 1001 SETTINGS force_data_skipping_indices = 'idx'"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_race_index"

# `alter_column_secondary_index_mode = 'drop'` generates `DROP_INDEX` with `clear = true`.
# It needs the same barrier so the clear mutation cannot miss an old-snapshot index file.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_race_index_drop (a UInt64, y UInt64 ALIAS greatest(a, * EXCEPT y), INDEX idx y TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, alter_column_secondary_index_mode = 'drop'"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT ${FP}"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_race_index_drop (a) VALUES (1)" > "${CLICKHOUSE_TMP}/05024_index_drop_insert.log" 2>&1 &
INSERT_PID=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
$CLICKHOUSE_CLIENT --lock_acquire_timeout 1 -q "ALTER TABLE t_race_index_drop ADD COLUMN b UInt64 DEFAULT a + 1000" 2>&1 \
    | grep -o 'DEADLOCK_AVOIDED' | head -1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT ${FP}"
wait "${INSERT_PID}"
INSERT_PID=
$CLICKHOUSE_CLIENT --alter_sync 2 -q "ALTER TABLE t_race_index_drop ADD COLUMN b UInt64 DEFAULT a + 1000"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_race_index_drop WHERE y = 1001 SETTINGS force_data_skipping_indices = 'idx'"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_race_index_drop"
