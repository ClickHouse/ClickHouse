#!/usr/bin/env bash
# Tags: no-msan
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command

# How the shared-memory regions of an executable UDF are accounted for. A region is charged to one
# memory tracker at a time: to the query while a query uses it, and to the server while a pooled
# worker holding it sits idle in the pool. Each scenario runs in a `clickhouse-local` of its own,
# so it starts from a process that holds no region and no charge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

function shm_function()
{
    # name, type, the options that make it what it is, the command
    echo "<function><type>$2</type><name>$1</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>"
    echo "<use_shared_memory>1</use_shared_memory>$3<command>$4</command></function>"
}

{
    shm_function shm_pool executable_pool "<shared_memory_size>1048576</shared_memory_size>" shm_udf.py
    shm_function shm_control executable "<shared_memory_size>8388608</shared_memory_size>" shm_udf.py
    shm_function shm_pipeline executable \
        "<shared_memory_size>8388608</shared_memory_size><shared_memory_pipeline>1</shared_memory_pipeline>" shm_udf.py
    shm_function shm_pool_pid executable_pool "<shared_memory_size>786432</shared_memory_size>" "shm_udf.py --report-pid"
    shm_function shm_pipeline_pool_partial executable_pool \
        "<shared_memory_size>786432</shared_memory_size><shared_memory_pipeline>1</shared_memory_pipeline>" shm_udf.py
    shm_function shm_idle executable_pool "<pool_size>1</pool_size><shared_memory_size>67108864</shared_memory_size>" shm_udf.py
    shm_function shm_pipeline_idle executable_pool \
        "<pool_size>1</pool_size><shared_memory_size>33554432</shared_memory_size><shared_memory_pipeline>1</shared_memory_pipeline>" shm_udf.py
    shm_function shm_pipeline_grow executable_pool \
        "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size><shared_memory_pipeline>1</shared_memory_pipeline>" shm_udf_grow.py
    shm_function shm_busy_chatty executable_pool "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size>" shm_udf_busy_chatty.py
} | shm_functions

echo "--- the region of a pooled worker is charged to the query that uses it"
shm_local "
    SELECT shm_pool(1) FORMAT Null SETTINGS max_memory_usage = 524288, max_untracked_memory = 0;
"

echo "--- both regions of the pipelined mode are charged to the query"
# Both functions use 8 MiB regions. The limit leaves almost 8 MiB for the one-region control's query
# overhead, but it is one byte short of the 16 MiB the two regions of the pipelined mode cost.
shm_local "
    SELECT shm_control(1) SETTINGS max_memory_usage = 16777215, max_untracked_memory = 0;
    SELECT shm_pipeline(1) FORMAT Null SETTINGS max_memory_usage = 16777215, max_untracked_memory = 0;
"

echo "--- a pooled region is counted once, not on every borrow"
# `ExecutableUDFSharedMemoryAllocatedBytes` tracks what was actually allocated, in whole pages like
# the charge, not what each borrow was charged.
shm_local "
    SELECT shm_pool(1);
    SELECT shm_pool(2); SELECT shm_pool(3); SELECT shm_pool(4); SELECT shm_pool(5);
    SELECT value = $(shm_pages 1048576) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';
"

echo "--- a borrow refused by the memory limit leaves the pool as it was"
# The first borrow is refused before it creates anything. Once a worker and its region exist, a
# borrow that cannot afford the region is refused while charging it, before any request reaches the
# worker: the pool has to hand back the very same process, with the very same region - the worker
# answers into the region it inherited, so a worker kept with its region dropped would have the next
# borrow read its answer out of a region the worker never writes to.
shm_local "
    SELECT shm_pool_pid(1) FORMAT Null SETTINGS max_memory_usage = 524288, max_untracked_memory = 0;
    SELECT count() FROM shm_regions;

    CREATE TABLE worker ENGINE = Memory AS SELECT shm_pool_pid(1) AS pid SETTINGS max_memory_usage = 10485760, max_untracked_memory = 0;
    CREATE TABLE regions_with_worker ENGINE = Memory AS SELECT inode, size FROM shm_regions;
    SELECT count() FROM regions_with_worker;

    SELECT shm_pool_pid(1) FORMAT Null SETTINGS max_memory_usage = 524288, max_untracked_memory = 0;
    SELECT (SELECT arraySort(groupArray((inode, size))) FROM shm_regions) = (SELECT arraySort(groupArray((inode, size))) FROM regions_with_worker);
    SELECT shm_pool_pid(1) = (SELECT pid FROM worker) SETTINGS max_memory_usage = 10485760, max_untracked_memory = 0;
    SELECT (SELECT arraySort(groupArray((inode, size))) FROM shm_regions) = (SELECT arraySort(groupArray((inode, size))) FROM regions_with_worker);
"

echo "--- a pipelined borrow refused halfway does not leave the region it created behind"
# Two regions of 768 KiB and a limit of 1 MiB: the first region is charged and created, the second
# is refused. The next borrow starts a process of its own and could not use the first one anyway.
shm_local "
    SELECT shm_pipeline_pool_partial(1) FORMAT Null SETTINGS max_memory_usage = 1048576, max_untracked_memory = 0;
    SELECT value = $(shm_pages 786432) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';
    SELECT count() FROM shm_regions;
"

echo "--- the region of an idle pooled worker is charged to the server, exactly once"
# Between invocations there is no query to charge: the charge is handed over to the server, which is
# what makes the region count against `max_server_memory_usage` while the worker sits in the pool. A
# second invocation borrows the same worker and region, and the charge comes back as exactly what it
# was. Dropping the pool releases the worker, its region and the charge.
shm_local "
    SELECT shm_idle(1);
    SELECT count() FROM shm_regions WHERE size = 67108864;
    SELECT value FROM shm_pooled;
    SELECT shm_idle(1);
    SELECT value FROM shm_pooled;
    SYSTEM RELOAD FUNCTION shm_idle;
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions;
"

echo "--- both regions of an idle pipelined worker are charged to the server"
shm_local "
    SELECT shm_pipeline_idle(1);
    SELECT count() FROM shm_regions WHERE size = 33554432;
    SELECT value FROM shm_pooled;
    SYSTEM RELOAD FUNCTION shm_pipeline_idle;
    SELECT value FROM shm_pooled;
    SELECT count() FROM shm_regions;
"

echo "--- a pipelined pooled worker keeps the region it grew, and the other one as it is"
# An executable function passes one block per call, so only the region the first block lands in
# grows; the other one stays at the configured 4096 bytes. The idle charge is what the worker
# actually holds - one grown and one base-sized region, in whole pages - and later borrows use both
# regions as they are, without growing anything again.
shm_local "
    SELECT countIf(shm_pipeline_grow(number) = toString(number)) FROM numbers(8000) SETTINGS max_block_size = 2000;
    SELECT value > 0 FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';
    CREATE TABLE regions_after ENGINE = Memory AS SELECT inode, size FROM shm_regions;
    CREATE TABLE growths_after ENGINE = Memory AS SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';
    SELECT count(), min(size) = 4096, max(size) > 4096 FROM regions_after;
    SELECT value = (SELECT sum(intDiv(size + ${SHM_PAGE} - 1, ${SHM_PAGE}) * ${SHM_PAGE}) FROM regions_after) FROM shm_pooled;

    SELECT countIf(shm_pipeline_grow(number) = toString(number)) FROM numbers(8000) SETTINGS max_block_size = 2000;
    SELECT (SELECT arraySort(groupArray((inode, size))) FROM shm_regions) = (SELECT arraySort(groupArray((inode, size))) FROM regions_after);
    SELECT value = (SELECT sum(intDiv(size + ${SHM_PAGE} - 1, ${SHM_PAGE}) * ${SHM_PAGE}) FROM regions_after) FROM shm_pooled;
    SELECT value = (SELECT value FROM growths_after) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';

    SYSTEM RELOAD FUNCTION shm_pipeline_grow;
    SELECT value FROM shm_pooled;
"

echo "--- a worker discarded for its stdout still reports its CPU and memory"
# The command burns CPU answering and then leaves a byte past its response frame, so the worker is
# discarded rather than returned to the pool. The borrow's CPU and peak resident set are read out of
# `/proc/<pid>`, and the teardown takes that away - a zombie has no `VmHWM`, and a reaped pid has
# nothing at all - so they have to be read before it.
shm_local "
    SELECT shm_busy_chatty(1) FORMAT Null;
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
    SELECT value >= 10000 FROM system.events WHERE event = 'ExecutableUserDefinedFunctionUserTimeMicroseconds';
    SELECT value > 0 FROM system.events WHERE event = 'ExecutableUserDefinedFunctionPeakMemoryByteSeconds';
"
