#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# The scrub between borrowers writes every page of a pooled region, and a page the command freed
# inside the file is allocated again by that write. The command here hides such holes from the
# footprint: it frees every page past its answer and commits as many pages past the end of the
# file, so the file keeps its length and its number of committed pages and the borrow's checks see
# a region within the cap. The scrub for another user refills the holes and takes the region to
# almost twice the cap: that borrow fails, with the worker and its region, instead of carrying on
# with pages nobody was charged for. The next borrow starts a fresh worker.
#
# The pool lives as long as the process does, so both users come to the same one: one
# `clickhouse-local` listens on HTTP and sends itself the other user's queries, as in 05333. `url`
# does not retry (`shm_as_user`): a retry would land on the fresh worker and hide the failed borrow.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# Its commands lay the region out page by page, in base pages.
shm_require_base_pages

{
    echo "<function><type>executable_pool</type><name>shm_hidden_holes</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>65536</shared_memory_size>"
    echo "<command>shm_udf_region.py --hide-holes</command></function>"
} | shm_functions

shm_local_listening "
    SELECT shm_hidden_holes(1);
    $(shm_as_user other "SELECT shm_hidden_holes(1)")
    $(shm_as_user other "SELECT shm_hidden_holes(1)")
"
shm_log_contains "past shared_memory_max_size (65536 bytes), while the region was being borrowed"
