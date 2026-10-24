#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# A growth of the region commits only the pages past what the server itself last committed, not the
# file from its start: pages the command freed inside the file stay free. The command here frees
# every page but the first, commits as many pages past the length it then asks for, and asks for a
# region of the cap. Committing the whole file again would refill the freed pages on top of the ones
# past the end and take the footprint past the cap; committing only the new part keeps it at the cap,
# so the query succeeds.

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

shm_local "
    SELECT shm_hide_holes_then_ask(1);
    SELECT size, committed <= 32768 FROM shm_regions;"
