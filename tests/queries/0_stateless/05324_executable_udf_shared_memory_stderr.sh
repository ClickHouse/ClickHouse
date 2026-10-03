#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# What a pooled command of the shared-memory transport writes besides its response frame, between
# borrows: to stderr after it answers, or past the frame on its stdout. A pooled worker has to go back
# to the pool only at a clean protocol boundary, output one borrow left must not be attributed to the
# next, and a chatty or flooding command must never leave a query hanging. Each scenario runs in a
# `clickhouse-local` of its own, so its log and its pool are its own. The pooled commands answer with
# their pid, which tells a reused worker from a fresh one; `shm_wait.sh` waits for a state of a worker
# that nothing in SQL can observe, instead of a sleep long enough to hope for it. How a single query
# reacts to what its own command writes is in `05325_executable_udf_shared_memory_stderr_reaction`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

STRAY_BYTE_MARKER="$SHM_UDF_WORK/stray_byte_written"

function shm_function()
{
    # name, type, the options that make it what it is, the command
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>"
    echo "<use_shared_memory>1</use_shared_memory>$3<command>$4</command></function>"
}

{
    shm_function shm_late_exit executable_pool "<pool_size>1</pool_size><shared_memory_size>65536</shared_memory_size>" shm_udf_stderr_then_late_exit.py
    shm_function shm_chatty executable_pool "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size>" shm_udf_chatty.py
    shm_function shm_flood_after_gap_throw executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>throw</stderr_reaction><command_read_timeout>5000</command_read_timeout><shared_memory_size>4096</shared_memory_size>" \
        "shm_udf_stderr_flood_after_gap.py --gap 2"
    shm_function shm_flood_after_gap_none executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>none</stderr_reaction><command_read_timeout>5000</command_read_timeout><shared_memory_size>4096</shared_memory_size>" \
        shm_udf_stderr_flood_after_gap.py
    shm_function shm_flood_none executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>none</stderr_reaction><command_read_timeout>5000</command_read_timeout><shared_memory_size>4096</shared_memory_size>" \
        shm_udf_stderr_flood_none.py
    shm_function shm_stray_byte executable_pool "<pool_size>1</pool_size><shared_memory_size>12288</shared_memory_size>" \
        "shm_udf_stray_byte_after_probe.py --marker $STRAY_BYTE_MARKER"
    shm_function shm_chatty_stderr_log executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>log_last</stderr_reaction><shared_memory_size>16777216</shared_memory_size>" shm_udf_chatty_stderr.py
    shm_function shm_flooding_stdout executable_pool "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size>" shm_udf_flooding_stdout.py
    shm_function shm_flooding_stderr executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>none</stderr_reaction><shared_memory_size>4096</shared_memory_size>" shm_udf_flooding_stderr.py
    shm_function shm_busy_chatty_throw executable_pool \
        "<pool_size>1</pool_size><stderr_reaction>throw</stderr_reaction><shared_memory_size>4096</shared_memory_size>" shm_udf_busy_chatty.py
    shm_function shm_quiet_stderr executable_pool "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size>" shm_udf_quiet_stderr.py
} | shm_functions

echo "--- a worker that died in the pool has its last words reported"
# It answers, waits out the hand-back probe, writes to stderr and exits. Nobody reads its pipes at
# that point: the next borrow finds it dead, starts a replacement, and reports what it wrote. The
# first call is made right where its pid is waited on: a worker that is about to die must not be
# borrowed by anything else in between.
shm_local "
    SELECT * FROM executable('shm_wait.sh exited', TSV, 'ok UInt8', (SELECT shm_late_exit(0)));
    SELECT length(shm_late_exit(1)) > 0;
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryCalls';
"
shm_log_contains "exited while it was idle in the pool, after writing to its stderr"
shm_log_contains "last words of the worker"

echo "--- a worker that leaves a byte on its stdout is discarded"
# The query it answered notices nothing, but the next borrow would read the byte as the status of its
# own response. So the worker does not go back to the pool, and the discard is counted and logged.
shm_local "
    SELECT shm_chatty(1); SELECT shm_chatty(1); SELECT shm_chatty(1);
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"
shm_log_contains "left unread output on its stdout after answering"

echo "--- a byte written after the hand-back probe does not reach the next query"
# The probe is one instant, so the byte is on the pipe when the next query borrows the worker. Before
# its first request it is provably not this query's: the worker goes with its region - it still holds
# a writable descriptor to it - and a replacement on a fresh region answers.
shm_local "
    SELECT shm_stray_byte(1);
    CREATE TABLE regions_before ENGINE = Memory AS SELECT inode FROM shm_regions;
    SELECT * FROM executable('shm_wait.sh file $STRAY_BYTE_MARKER', TSV, 'ok UInt8', (SELECT 0));
    SELECT shm_stray_byte(2);
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
    SELECT count(), countIf(inode IN (SELECT inode FROM regions_before)) FROM shm_regions;
"
shm_log_contains "had unread output on its stdout when it was borrowed"

echo "--- stderr a previous borrow left is not thrown at the next query"
# Under \`throw\`: the command answers, waits out the hand-back probe and writes two pipefuls to stderr.
# Those bytes are the earlier query's, which has already succeeded, and the command is blocked in the
# middle of writing them: nothing tells when it will write the rest. So the next borrow does not
# build on it - the worker is discarded with its region, the stderr is logged against it, and a
# replacement answers. The query does not fail.
shm_local "
    CREATE TABLE pids (n UInt8, pid String) ENGINE = Memory;
    INSERT INTO pids SELECT 0, shm_flood_after_gap_throw(0);
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT pid FROM pids WHERE n = 0));
    INSERT INTO pids SELECT 1, shm_flood_after_gap_throw(1);
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT pid FROM pids WHERE n = 1));
    INSERT INTO pids SELECT 2, shm_flood_after_gap_throw(2);
    SELECT count(), uniqExact(pid) FROM pids;
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"
shm_log_contains "had unread output on its stderr when it was borrowed under stderr_reaction 'throw'"

echo "--- under none, a worker flooding stderr is never left blocked"
# Whether the flood comes after a quiet gap longer than the hand-back drain, or right after the
# answer. The read loop polls stderr alongside stdout, so the query waiting for a response drains the
# command writing it; a worker left blocked in \`write\` would fail the query on its read timeout.
shm_local "
    CREATE TABLE pids (pid String) ENGINE = Memory;
    INSERT INTO pids SELECT shm_flood_after_gap_none(0);
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT any(pid) FROM pids));
    INSERT INTO pids SELECT shm_flood_after_gap_none(1);
    SELECT * FROM executable('shm_wait.sh blocked', TSV, 'ok UInt8', (SELECT any(pid) FROM pids));
    INSERT INTO pids SELECT shm_flood_after_gap_none(2);
    SELECT count(), uniqExact(pid) FROM pids;

    CREATE TABLE pids_flood (pid String) ENGINE = Memory;
    INSERT INTO pids_flood SELECT shm_flood_none(0);
    INSERT INTO pids_flood SELECT shm_flood_none(1);
    INSERT INTO pids_flood SELECT shm_flood_none(2);
    SELECT count(), uniqExact(pid) FROM pids_flood;

    -- Megabytes before the answer: the bytes have to be read off the pipe and dropped.
    CREATE TABLE pids_before (pid String) ENGINE = Memory;
    INSERT INTO pids_before SELECT shm_flooding_stderr(1);
    INSERT INTO pids_before SELECT shm_flooding_stderr(1);
    INSERT INTO pids_before SELECT shm_flooding_stderr(1);
    SELECT count(), uniqExact(pid) FROM pids_before;
"

echo "--- a line on stderr with the answer under log_last keeps the worker"
# It is logged against the query that caused it, and the worker goes back to the pool.
shm_local "
    CREATE TABLE pids (pid String) ENGINE = Memory;
    INSERT INTO pids SELECT shm_chatty_stderr_log(1);
    INSERT INTO pids SELECT shm_chatty_stderr_log(1);
    INSERT INTO pids SELECT shm_chatty_stderr_log(1);
    SELECT count(), uniqExact(pid) FROM pids;
"
grep -cF "Executable generates stderr at the end: done" "$SHM_UDF_WORK/local.log"

echo "--- a worker flooding its stdout after the answer is discarded without hanging"
shm_local "
    CREATE TABLE pids (pid String) ENGINE = Memory;
    INSERT INTO pids SELECT shm_flooding_stdout(1);
    INSERT INTO pids SELECT shm_flooding_stdout(1);
    INSERT INTO pids SELECT shm_flooding_stdout(1);
    SELECT count(), uniqExact(pid) FROM pids;
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"
shm_log_contains "left unread output on its stdout after answering"

echo "--- a worker discarded for its stdout does not fail a query under throw"
# The discard looks at stderr to report what the command left there; finding nothing is not stderr.
shm_local "
    SELECT match(shm_busy_chatty_throw(1), '^[0-9]+\$');
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"

echo "--- a command may close its stderr"
# A closed stderr polls as a hangup forever, with nothing to read: that is not output left behind.
shm_local "
    CREATE TABLE pids (pid String) ENGINE = Memory;
    INSERT INTO pids SELECT shm_quiet_stderr(1);
    INSERT INTO pids SELECT shm_quiet_stderr(1);
    INSERT INTO pids SELECT shm_quiet_stderr(1);
    SELECT count(), uniqExact(pid) FROM pids;
    SELECT count() FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"
