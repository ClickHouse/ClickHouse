#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# How the shared-memory region of an executable UDF are accounted for. A region is charged to one
# memory tracker at a time: to the query while a query uses it, and to the server while a pooled
# worker holding it sits idle in the pool. Each scenario runs in a `clickhouse-local` of its own,
# so it starts from a process that holds no region and no charge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    shm_function shm_pool executable_pool "<shared_memory_size>1048576</shared_memory_size>" shm_udf.py
    shm_function shm_pool_pid executable_pool "<shared_memory_size>786432</shared_memory_size>" "shm_udf.py --report-pid"
    shm_function shm_idle executable_pool "<pool_size>1</pool_size><shared_memory_size>67108864</shared_memory_size>" shm_udf.py
    shm_function shm_pool_grow executable_pool \
        "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size><shared_memory_max_size>1048576</shared_memory_max_size>" "shm_udf.py --echo --answer-at-start"
    shm_function shm_busy_chatty executable_pool "<pool_size>1</pool_size><shared_memory_size>4096</shared_memory_size>" "shm_udf_noisy.py --report-pid --burn-cpu --stdout-after-frame byte"
} | shm_functions

echo "--- the region of a pooled worker is charged to the query that uses it"
shm_local "
    SELECT shm_pool(1) FORMAT Null SETTINGS max_memory_usage = 524288, max_untracked_memory = 0;
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

echo "--- the region of an idle pooled worker is charged to the server, exactly once"
# Between invocations there is no query to charge: the charge is handed over to the server, which is
# what makes the region count against `max_server_memory_usage` while the worker sits in the pool. A
# second invocation borrows the same worker and region, and the charge comes back as exactly what it
# was. Dropping the pool releases the worker, its region and the charge. The charge is also counted
# in \`MemoryTrackingUnmeasured\`, which the memory worker adds to its measurement - none of the
# measurements it corrects the server-wide tracker with sees the pages of a region.
shm_local "
    SELECT shm_idle(1);
    SELECT count() FROM shm_regions WHERE size = 67108864;
    SELECT value FROM shm_pooled;
    SELECT value FROM system.metrics WHERE metric = 'MemoryTrackingUnmeasured';
    SELECT shm_idle(1);
    SELECT value FROM shm_pooled;
    SYSTEM RELOAD FUNCTION shm_idle;
    SELECT value FROM shm_pooled;
    SELECT value FROM system.metrics WHERE metric = 'MemoryTrackingUnmeasured';
    SELECT count() FROM shm_regions;
"

echo "--- a pooled worker keeps the region it grew"
# The region is sealed against shrinking, so the worker keeps the size of the largest chunk it held.
# The idle charge is what the worker actually holds, in whole pages, and later borrows use the
# region as it is, without growing it again.
shm_local "
    SELECT countIf(shm_pool_grow(number) = toString(number)) FROM numbers(8000) SETTINGS max_block_size = 2000;
    SELECT value > 0 FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';
    CREATE TABLE regions_after ENGINE = Memory AS SELECT inode, size FROM shm_regions;
    CREATE TABLE growths_after ENGINE = Memory AS SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';
    SELECT count(), max(size) > 4096 FROM regions_after;
    SELECT value = (SELECT sum(intDiv(size + ${SHM_PAGE} - 1, ${SHM_PAGE}) * ${SHM_PAGE}) FROM regions_after) FROM shm_pooled;

    SELECT countIf(shm_pool_grow(number) = toString(number)) FROM numbers(8000) SETTINGS max_block_size = 2000;
    SELECT (SELECT arraySort(groupArray((inode, size))) FROM shm_regions) = (SELECT arraySort(groupArray((inode, size))) FROM regions_after);
    SELECT value = (SELECT sum(intDiv(size + ${SHM_PAGE} - 1, ${SHM_PAGE}) * ${SHM_PAGE}) FROM regions_after) FROM shm_pooled;
    SELECT value = (SELECT value FROM growths_after) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryRegionGrowths';

    SYSTEM RELOAD FUNCTION shm_pool_grow;
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
