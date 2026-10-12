#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: the commands are waited for through `/proc`

# What a pooled command of the pipe transport writes besides its rows, and when: with them, after them,
# or once it is back in the pool. A pooled worker has to go back to the pool only at a clean boundary,
# output one borrow left must not be read or attributed by the next, and a chatty command must never
# be left blocked. Each scenario runs in a `clickhouse-local` of its own, so its log and its pool are
# its own. The commands answer with their pid, which tells a reused worker from a fresh one. A command
# that is to write something once the query it answered is over waits for the next statement to say
# so (`shm_wait.sh touch`, `go_signal.py`), and `shm_wait.sh` waits for a state of a worker that
# nothing in SQL can observe - instead of sleeps long enough to hope for either. The shared-memory
# twin of this test is `05324_executable_udf_shared_memory_stderr`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

GO="$SHM_UDF_WORK/go"
MARKER="$SHM_UDF_WORK/marker"

function pipe_function()
{
    # name, return type, format, the options that make it what it is, the command
    echo "<function><type>executable_pool</type><name>$1</name><return_type>$2</return_type>"
    echo "<argument><type>UInt64</type></argument><format>$3</format><pool_size>1</pool_size>$4<command>$5</command></function>"
}

{
    pipe_function pipe_flood_none UInt64 TabSeparated \
        "<stderr_reaction>none</stderr_reaction><command_read_timeout>5000</command_read_timeout>" \
        "pipe_pool_pid.py --go $GO --after-go stderr-flood"
    pipe_function pipe_flood_throw UInt64 TabSeparated \
        "<stderr_reaction>throw</stderr_reaction><command_read_timeout>5000</command_read_timeout>" \
        "pipe_pool_pid.py --go $GO --after-go stderr-flood --bytes 49152 --marker $MARKER"
    pipe_function pipe_stderr_after_rows String TabSeparated "<stderr_reaction>throw</stderr_reaction>" "pipe_udf.py --stderr-with-rows"
    pipe_function pipe_late_exit UInt64 TabSeparated "" "pipe_pool_pid.py --go $GO --after-go exit"
    pipe_function pipe_stderr_then_late_exit UInt64 TabSeparated "" "pipe_pool_pid.py --go $GO --after-go last-words"
    pipe_function pipe_native_overproduce String Native "" pipe_pool_native_overproduce.py
    pipe_function pipe_late_stdout UInt64 TabSeparated "" "pipe_pool_pid.py --go $GO --after-go stdout-row --marker $MARKER"
    pipe_function pipe_chatty UInt64 TabSeparated "" "pipe_pool_pid.py --extra-newline"
    pipe_function pipe_pid_then_stderr String TabSeparated "<stderr_reaction>log_last</stderr_reaction>" "pipe_pool_pid.py --stderr-after-rows"
} | shm_functions

echo "--- under none, a worker flooding stderr once it is back in the pool is never left blocked"
# Two pipefuls after the hand-back drain: a check that only looks at the pipe when the worker is handed
# back sees nothing. The read loop of the next borrow polls stderr alongside stdout, so the query
# waiting for the answer drains the command writing it; a worker left blocked in \`write\` would fail
# the query on its read timeout.
shm_local "
    CREATE TABLE pids (pid UInt64) ENGINE = Memory;
    INSERT INTO pids SELECT pipe_flood_none(0);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT any(pid) FROM pids));
    INSERT INTO pids SELECT pipe_flood_none(1);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT any(pid) FROM pids));
    INSERT INTO pids SELECT pipe_flood_none(2);
    SELECT count(), uniqExact(pid) FROM pids;
"

echo "--- under throw, stderr a previous borrow left is not thrown at the next query"
# The bytes are the earlier query's, which has already succeeded. The next borrow does not build on
# that worker: nothing tells whether the command has finished writing them, and a tail written during
# the next request could not be told from that request's own stderr. So the worker is discarded and a
# replacement answers - every query gets a process of its own, and none of them fails.
shm_local "
    CREATE TABLE pids (pid UInt64) ENGINE = Memory;
    INSERT INTO pids SELECT pipe_flood_throw(0);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh file $MARKER', TSV, 'ok UInt8', (SELECT 0));
    INSERT INTO pids SELECT pipe_flood_throw(1);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh file $MARKER', TSV, 'ok UInt8', (SELECT 0));
    INSERT INTO pids SELECT pipe_flood_throw(2);
    SELECT count(), uniqExact(pid) FROM pids;
"
shm_log_contains "borrowed under stderr_reaction 'throw', so it wrote after the response of an earlier invocation"

echo "--- under throw, stderr written with the rows fails the query that caused it"
# A pooled worker that satisfied the row count goes back to the pool without being waited for, so the
# hand-back probe is the last look at its stderr - and the query has its rows by then. The command
# puts its diagnostic on the pipe before it flushes its rows (see the script), so the look finds it
# every time.
shm_local "
    SELECT pipe_stderr_after_rows(1);
"
shm_output_contains "the command complains"

echo "--- a worker that exited in the pool is replaced"
# Nobody waits for a pooled process between borrows, so the next query is the first to find out -
# and it must not find out by failing its own first write to a closed stdin.
shm_local "
    CREATE TABLE worker ENGINE = Memory AS SELECT pipe_late_exit(number) AS pid FROM numbers(1);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh exited', TSV, 'ok UInt8', (SELECT pid FROM worker));
    SELECT pipe_late_exit(1) != (SELECT pid FROM worker);
"
shm_log_contains "exited while it was idle in the pool"

echo "--- a worker that died in the pool has its last words reported"
# What it wrote before dying is reported against the process before its pipes are closed with it.
shm_local "
    CREATE TABLE worker ENGINE = Memory AS SELECT pipe_stderr_then_late_exit(number) AS pid FROM numbers(1);
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh exited', TSV, 'ok UInt8', (SELECT pid FROM worker));
    SELECT pipe_stderr_then_late_exit(1) != (SELECT pid FROM worker);
"
shm_log_contains "exited while it was idle in the pool, after writing to its stderr"
shm_log_contains "last words of the worker"

echo "--- extra rows inside one Native block fail the query and cost the worker"
# A row format never hands over more than \`max_block_size\` rows at once, so overproduction usually
# shows up as bytes left in the pipe. A block format hands over the command's block whole: the extra
# row is inside the chunk, the pipe is clean, and only the row count can catch it - in the source,
# before the worker goes back to the pool as if it had answered correctly.
shm_local "
    SELECT pipe_native_overproduce(number) FROM numbers(3) FORMAT Null;
    SELECT pipe_native_overproduce(number) FROM numbers(3) FORMAT Null;
    SELECT pipe_native_overproduce(number) FROM numbers(3) FORMAT Null;
"
shm_output_contains "but the command produced more"

echo "--- a row written once the worker is back in the pool is not read by the next query"
# The pipe transport has no framing that would tell a stale row from the next query's own, so the
# next borrow must not start reading on such a worker: before anything is sent the row is provably not
# its own, the worker is discarded, and a fresh one answers - never with \`999999\`.
shm_local "
    CREATE TABLE worker ENGINE = Memory AS SELECT pipe_late_stdout(number) AS pid FROM numbers(1);
    SELECT pid != 999999 FROM worker;
    SELECT * FROM executable('shm_wait.sh touch $GO', TSV, 'ok UInt8', (SELECT 0));
    SELECT * FROM executable('shm_wait.sh file $MARKER', TSV, 'ok UInt8', (SELECT 0));
    SELECT pipe_late_stdout(1) AS pid, pid != 999999, pid != (SELECT pid FROM worker) FORMAT TSV;
" | cut -f2-
shm_log_contains "had unread output on its stdout when it was borrowed"

echo "--- a byte written together with the rows does not cost the worker"
# It is read into the query's own buffer along with the rows - the reader reads ahead in blocks - and
# dies with it, so the worker is still at a usable boundary. Taking bytes a format reader merely holds
# for a dirty worker would turn \`executable_pool\` into a process per call: one worker, eight calls.
shm_local "
    CREATE TABLE pids (pid UInt64) ENGINE = Memory;
    INSERT INTO pids SELECT pipe_chatty(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_chatty(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_chatty(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_chatty(number) FROM numbers(1);
    SELECT count(), uniqExact(pid) FROM pids;
"

echo "--- under log_last, a line written after the rows is logged and the worker is kept"
# Under \`throw\` it would be a verdict on a query that has already succeeded; under a \`log*\` reaction it
# is a log line, and discarding the worker for it would make every command that logs after its rows a
# process per call.
shm_local "
    CREATE TABLE pids (pid String) ENGINE = Memory;
    INSERT INTO pids SELECT pipe_pid_then_stderr(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_pid_then_stderr(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_pid_then_stderr(number) FROM numbers(1);
    INSERT INTO pids SELECT pipe_pid_then_stderr(number) FROM numbers(1);
    SELECT count(), uniqExact(pid) FROM pids;
"
shm_log_contains "logging right after the rows"
