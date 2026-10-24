#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# `command_read_timeout` bounds the time a whole response frame of the shared-memory protocol takes
# to arrive, not the gap between two of its bytes. The command writes its frame a byte every 0.2
# seconds - well within the timeout of 1 second every time - and the frame takes several seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    echo "<function><type>executable_pool</type><name>shm_drip</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>4096</shared_memory_size>"
    echo "<command_read_timeout>1000</command_read_timeout><command_termination_timeout>0</command_termination_timeout>"
    echo "<command>shm_udf_broken.py --drip 0.2 40</command></function>"
} | shm_functions

echo "--- a frame that trickles in is timed out as a whole"
shm_local "
    SELECT shm_drip(1);
"
