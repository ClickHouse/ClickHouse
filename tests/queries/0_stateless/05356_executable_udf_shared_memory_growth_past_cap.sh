#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A growth of the region is bounded before it commits anything, but the bound cannot see pages the
# command freed inside the file and replaced with as many pages past its end: the growth commits the
# freed ones again, and the footprint ends up past `shared_memory_max_size`. The cap is checked again
# after the growth, so the query fails rather than going on with a region past it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# Its command lays the region out page by page, in base pages.
shm_require_base_pages

{
    echo "<function><type>executable_pool</type><name>shm_hide_holes_then_ask</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>16384</shared_memory_size>"
    echo "<shared_memory_max_size>32768</shared_memory_max_size>"
    echo "<command>shm_udf_region.py --hide-holes-then-ask 32768</command></function>"
} | shm_functions

shm_local "SELECT shm_hide_holes_then_ask(1);"
shm_output_contains "past shared_memory_max_size (32768 bytes): its process freed pages inside the region"
