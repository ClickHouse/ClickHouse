#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A discarded worker whose process tree does not go away by itself. The command answers with a
# stray byte after its response, so the hand-back discards it; it ignores `SIGTERM`, and so does a
# descendant it forked at startup that holds nothing but the inherited descriptor of the region.
# The server stops charging for the region when it drops the worker, and anything still alive that
# holds the descriptor could write into the region after that, with nobody charged for the pages.
# So a shared-memory command runs in a process group of its own, and once the grace period
# (`command_termination_timeout`) is spent the whole group is sent `SIGKILL` before the region goes:
# by the time the query is over, neither the worker nor its descendant is running.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    echo "<function><type>executable_pool</type><name>shm_lingering_group</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>"
    echo "<command_termination_timeout>1</command_termination_timeout><check_exit_code>0</check_exit_code>"
    echo "<command>shm_udf_lingering_group.py $SHM_UDF_WORK</command></function>"
} | shm_functions

shm_local "
    SELECT shm_lingering_group(1);
    SELECT sum(value) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryDirtyChannelDiscards';
"

for name in worker descendant; do
    if [[ -f "$SHM_UDF_WORK/$name" ]]; then
        echo "$name: $(shm_process_state "$(cat "$SHM_UDF_WORK/$name")")"
    else
        echo "$name: no pid"
    fi
done
