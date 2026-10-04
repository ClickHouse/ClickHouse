#!/usr/bin/env bash
# Tags: no-msan, no-darwin
# - no-msan: Memory Sanitizer cannot work with vfork, which starts the command
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The answer fails to parse while the source is reading it out of the region, so the source is torn
# down from the exception path rather than at the end of its answer. The region must not be left
# charged to a query that is over, and a pool of one must still serve the next call. The worker is
# discarded with its region - it stopped in the middle of a request - and the next call gets a new one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

{
    echo "<function><type>executable_pool</type><name>shm_echo</name><return_type>UInt64</return_type>"
    echo "<argument><type>String</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>"
    echo "<command>shm_udf.py --echo</command></function>"
} | shm_functions

shm_local "
    SELECT shm_echo('1');
    SELECT shm_echo(x) FROM values('x String', '2', 'not a number', '3');
    SELECT count() FROM shm_regions;
    SELECT value FROM system.metrics WHERE metric = 'MemoryTrackingUnmeasured';
    SELECT shm_echo('4');
    SELECT count() FROM shm_regions;
    SELECT (SELECT value FROM shm_pooled) = (SELECT value FROM system.metrics WHERE metric = 'MemoryTrackingUnmeasured');
"
