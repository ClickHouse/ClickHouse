#!/usr/bin/env bash
# Tags: atomic-database, memory-engine, no-parallel
# Tag no-parallel: uses `SYSTEM ENABLE FAILPOINT infinite_sleep`, which is server-global and would
#   park every other sleeping query on the server, so it cannot run concurrently with other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

# Helper: wait until the refresh is running AND has read at least one row, so that a cancellation
# issued right after has a concrete pipeline to interrupt (see 04105_system_pause_view).
wait_running_with_progress() {
    local view_name=$1
    while [ "`$CLICKHOUSE_CLIENT -q "select status = 'Running' and read_rows > 0 from refreshes where view = '$view_name' -- $LINENO" | xargs`" != '1' ]
    do
        sleep 0.1
    done
}

# ---------------------------------------------------------------------------
# SYSTEM CANCEL VIEW records the cancellation as an exception.
# ---------------------------------------------------------------------------

# The disable below is the resume mechanism and only runs on the happy path; this trap covers an
# early exit, which would otherwise leave the failpoint parking every later sleep in the run.
trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT infinite_sleep" 2>/dev/null || true
' EXIT

# The cancel must reach a live `PipelineExecutor`, so park the refresh at `infinite_sleep`, which
# `sleepEachRow` hits inside `executor.execute()`.
$CLICKHOUSE_CLIENT -q "
    create table src (x Int64) engine Memory;
    insert into src select * from numbers(1);
    create materialized view c refresh every 1 year settings refresh_retries = 0 (x Int64) engine Memory empty as
        select x + sleepEachRow(0) as x from src settings max_block_size = 1, max_threads = 1;
    system enable failpoint infinite_sleep;
    system refresh view c;"

if ! timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT infinite_sleep PAUSE"
then
    echo "FAIL: refresh did not reach the infinite_sleep failpoint"
    exit 1
fi

# The WAIT is keyed on the failpoint name, not on this view, so confirm OUR refresh is the parked one.
wait_running_with_progress c

# The refresh is parked, so the cancel cannot be outrun; disabling the failpoint resumes it.
$CLICKHOUSE_CLIENT -q "
    system cancel view c;
    system disable failpoint infinite_sleep;"

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
