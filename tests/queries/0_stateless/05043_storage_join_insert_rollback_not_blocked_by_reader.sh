#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel -- uses a server-wide failpoint that would pause the persistent `Set`/`Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `Join` insert whose replay would fail must never publish its rows, even when a
# concurrent long-running reader holds the table's read lock. The insert takes the write lock
# before replaying its promoted backup, so it fails up front (`DEADLOCK_AVOIDED` when the reader
# holds the lock past `lock_acquire_timeout`, or `SET_SIZE_LIMIT_EXCEEDED` during the replay
# itself), and the rollback happens under the same write lock -- it cannot time out behind the
# reader, which previously could leave the failed rows visible until restart.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_reader_rollback;
    CREATE TABLE join_reader_rollback (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)
        SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
    INSERT INTO join_reader_rollback VALUES (1, 'committed');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_replay"

insert_out="$CLICKHOUSE_TMP/05043_insert_out"
reader_out="$CLICKHOUSE_TMP/05043_reader_out"

# This insert makes the table exceed `max_rows_in_join`, so it must fail without publishing
# anything. It promotes its backup file, then pauses before replaying it into the live state.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO join_reader_rollback SETTINGS lock_acquire_timeout = 1 VALUES (2, 'failed'), (3, 'failed')
" > "$insert_out" 2>&1 &
failing_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_replay PAUSE"

# A reader that holds the table's read lock for several seconds (the lock is taken when the join
# is cloned and held until the query finishes).
$CLICKHOUSE_CLIENT --query_id "05043_reader_$CLICKHOUSE_DATABASE" --query "
    SELECT count(), countIf(v = 'committed')
    FROM (SELECT number AS k, sleepEachRow(0.5) FROM numbers(6)) AS t
    ANY LEFT JOIN join_reader_rollback USING (k)
" > "$reader_out" 2>&1 &
reader_pid=$!

# Wait until the reader is running, so it holds the read lock when the insert resumes.
for _ in {1..100}
do
    started=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '05043_reader_$CLICKHOUSE_DATABASE'")
    [[ "$started" == "1" ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_replay"

wait $failing_pid
wait $reader_pid

grep -q -E 'DEADLOCK_AVOIDED|SET_SIZE_LIMIT_EXCEEDED' "$insert_out" && echo 'INSERT_FAILED'
cat "$reader_out"
rm -f "$insert_out" "$reader_out"

# The failed rows must not be visible, neither live nor after restoring the state from disk.
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_reader_rollback ORDER BY k"
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_reader_rollback;
    ATTACH TABLE join_reader_rollback;
"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_reader_rollback ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_reader_rollback"
