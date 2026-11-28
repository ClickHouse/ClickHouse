#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The query runs out of memory while it parses the answer out of the region: the command answered
# in full - the response frame was read and the output it names was counted - and what failed is
# the query's own allocation for the result column. That says nothing about the command, so a pool
# of one keeps its worker together with its region, and the next call is served by the same worker
# without a new region being allocated. (Over the pipes the worker has to go: the rest of the
# answer is still in flight.)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

shm_function shm_echo executable_pool "<pool_size>1</pool_size><shared_memory_size>67108864</shared_memory_size>" "shm_udf.py --echo" "<argument><type>String</type></argument>" | shm_functions

# The input, the region and a copy of the input fit the limit; the answer parsed out of the region
# - another copy, grown by doubling - does not.
shm_local "
    SELECT length(shm_echo(repeat(repeat('x', 1024), 24576)));
    SELECT length(shm_echo(repeat(repeat('x', 1024), 24576))) SETTINGS max_memory_usage = 134217728, max_untracked_memory = 0;
    SELECT value FROM system.events WHERE event = 'ExecutableUDFSharedMemoryOutputBytes';
    SELECT count() FROM shm_regions;
    SELECT value FROM shm_pooled;
    SELECT length(shm_echo(repeat('y', 100)));
    SELECT value = $(shm_pages 67108864) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryAllocatedBytes';
    SELECT count() FROM shm_regions;
"
