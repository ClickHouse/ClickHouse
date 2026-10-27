#!/usr/bin/env bash
# Tags: atomic-database, memory-engine, no-parallel

# Uses `SYSTEM ENABLE FAILPOINT refresh_mv_pause_after_executor_published`, which is server-global
# and would park every other refresh on the server, so it cannot run concurrently with other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="refresh_mv_pause_after_executor_published"

# Set session timezone to UTC to make all DateTime formatting and parsing use UTC, because refresh
# scheduling is done in UTC.
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --allow_materialized_view_with_bad_select=0 --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"

# Helper: wait until the view's status matches the expected one.
wait_status() {
    local view_name=$1
    local expected=$2
    while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = '$view_name' -- $LINENO" | xargs`" != "$expected" ]
    do
        sleep 0.1
    done
}

# `enabled` is read while the refresh is parked, before the DISABLE that resumes it, so 1 then 0 can
# only happen through a fire; `enabled = 0` alone would also be what an un-armed failpoint reads.
enabled() {
    $CLICKHOUSE_CLIENT -q "select enabled from system.fail_points where name = '$FP' settings max_rows_to_read = 0"
}

# ---------------------------------------------------------------------------
# SYSTEM CANCEL VIEW records the cancellation as an exception.
# ---------------------------------------------------------------------------

# The disable below is the resume mechanism and only runs on the happy path; this trap covers an
# early exit, which would otherwise leave the failpoint parking every later refresh in the run.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT '"$FP"'" 2>/dev/null || true
' EXIT

# The cancel must reach a live `PipelineExecutor`, so park the refresh thread right after it
# published the executor and released `executor_mutex`, which is where `interruptExecution` can
# take that mutex and cancel it. Parking there instead of inside the pipeline keeps the wait off
# every `IProcessor::work()` frame: no worker has started yet.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(1);
    create materialized view c refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory empty as
        select x from src;
    system enable failpoint $FP;"

echo "armed $(enabled)"

$CLICKHOUSE_CLIENT -q "system refresh view c;"

if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
then
    echo "FAIL: refresh did not reach the $FP failpoint"
    exit 1
fi

echo "parked $(enabled)"

# The WAIT is keyed on the failpoint name, not on this view, so confirm OUR refresh is the parked one.
wait_status c Running

# The park is before `executor.execute()`, so the refresh cannot have read anything yet.
# `execution.progress` is reset at the top of `executeRefreshUnlocked`, and the refresh thread is
# parked, so this is stable: a park moved to after `execute()` would read 1 for this one-row source.
echo "read_rows_at_park $($CLICKHOUSE_CLIENT -q "select read_rows from refreshes where view = 'c' -- $LINENO" | xargs)"

# The refresh is parked, so the cancel cannot be outrun; disabling the failpoint resumes it.
$CLICKHOUSE_CLIENT -q "
    system cancel view c;
    system disable failpoint $FP;"

wait_status c Scheduled

# `Cancelling refresh in ...` is logged only when the interrupt finds a non-null
# `execution.executor`, so it proves the cancel hit the running pipeline, not a finished attempt.
# Matching 'cancelled' distinguishes a cancellation from any other refresh failure.
$CLICKHOUSE_CLIENT -q "
    system flush logs text_log;
    select '<1: cancel during refresh records an exception>',
        (select exception != '' from refreshes where view = 'c'),
        (select position(exception, 'cancelled') > 0 from refreshes where view = 'c'),
        (select count() > 0 from system.text_log
            where event_date >= yesterday() and event_time >= now() - 600
                and message = 'Cancelling refresh in ' || currentDatabase() || '.c'
            settings max_rows_to_read = 0);
    drop table c;
    drop table src;"
