#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A growth of the shared-memory region settles the query's whole charge for the region against its
# footprint, not only the part the growth added. The command here commits pages past the end of the
# region's file while it holds the region (`fallocate` with `FALLOC_FL_KEEP_SIZE`), then asks for a
# larger region. Those pages are in the footprint the growth re-reads, and the growth is charged on
# top of them: under a memory limit that the region as created fits and the region with those pages
# does not, the growth is refused.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# 64 MiB of pages, 64 MiB past the start of the file: whole pages, also where `shmem` is backed by
# 2 MiB transparent huge pages.
FAR=67108864

{
    echo "<function><type>executable</type><name>shm_alloc</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>4096</shared_memory_size>"
    echo "<shared_memory_max_size>134217728</shared_memory_max_size>"
    echo "<command>shm_udf_region.py --alloc-beyond-eof $FAR $FAR</command></function>"
} | shm_functions

# 500 rows: the input fits into the first page, the input and the answer next to it do not, so the
# command asks for a larger region after it has committed its pages. The limit leaves the query
# plenty of memory of its own besides the region and the growth, and is far short of the pages the
# command committed.
LIMIT=33554432

echo "--- the growth is refused under a limit the pages the command committed do not fit"
# Only the code is checked, not what wraps it. The refused charge is taken off the query's tracker
# right after it is refused, but for that moment the tracker holds it, and another thread of the same
# query allocating then - the client polling the query's progress, say, which a debug build does
# often - hits the limit first; the query fails with that thread's `MEMORY_LIMIT_EXCEEDED` instead of
# the UDF's `UDF_EXECUTION_FAILED` around it. Either is the growth being refused - which the counters
# say for certain: the command was called, and the region did not grow.
GROWTH_EVENTS="SELECT sumIf(value, event = 'ExecutableUDFSharedMemoryCalls'),
    sumIf(value, event = 'ExecutableUDFSharedMemoryRegionGrowths') FROM system.events;"
shm_local "
    SELECT sum(length(shm_alloc(number))) FROM numbers(500)
    SETTINGS max_block_size = 500, max_memory_usage = $LIMIT, max_untracked_memory = 0;
    $GROWTH_EVENTS
" | sed -E 's/^error:.* (MEMORY_LIMIT_EXCEEDED)( .*)?$/\1/'

echo "--- and goes through without it"
shm_local "
    SELECT sum(length(shm_alloc(number))) FROM numbers(500) SETTINGS max_block_size = 500;
    $GROWTH_EVENTS
"
