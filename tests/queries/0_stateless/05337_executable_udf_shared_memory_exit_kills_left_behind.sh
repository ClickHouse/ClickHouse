#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A shared-memory command that exits as it should, on EOF, but leaves two descendants holding the
# inherited descriptor of the region: one in the command's process group, one that left it
# (`setsid`). The command is reaped only after its group is sent `SIGKILL`, so the first one does
# not outlive it - for `executable`, where the command exits at the end of the query, and for
# `executable_pool`, where it exits when the pool is torn down. The second one is out of reach, but
# the server frees the pages of the region's file as it drops the region, so the file that
# descendant still holds has no blocks left.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

mkdir -p "$SHM_UDF_WORK/executable" "$SHM_UDF_WORK/executable_pool"

{
    for type in executable executable_pool; do
        echo "<function><type>$type</type><name>shm_left_behind_$type</name><return_type>String</return_type>"
        echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
        echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>"
        echo "<command>shm_udf_left_behind.py $SHM_UDF_WORK/$type</command></function>"
    done
} | shm_functions

shm_local "
    SELECT shm_left_behind_executable(1);
    SELECT shm_left_behind_executable_pool(1);
"

for type in executable executable_pool; do
    dir="$SHM_UDF_WORK/$type"
    for name in worker in_group; do
        if [[ -f "$dir/$name" ]]; then
            echo "$type $name: $(shm_process_state "$(cat "$dir/$name")")"
        else
            echo "$type $name: no pid"
        fi
    done

    # The descendant that left the group reports what its descriptor still holds, once told to.
    touch "$dir/go"
    for _ in $(seq 1 100); do
        [[ -f "$dir/left_blocks" ]] && break
        sleep 0.1
    done
    echo "$type left_group blocks: $(cat "$dir/left_blocks" 2>/dev/null || echo 'no report')"
done
