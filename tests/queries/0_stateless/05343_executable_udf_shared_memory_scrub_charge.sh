#!/usr/bin/env bash
# Tags: no-darwin
# - no-darwin: shared-memory regions for executable UDFs are supported only on Linux

# Clearing a pooled region for a borrower other than the previous one is charged to the query for
# what it costs, not for the whole region a second time. The borrow is already charged for the
# region, and a region whose pages are all there commits nothing new when it is cleared: a query
# whose memory limit fits the region once borrows it, whoever borrowed it before.
#
# As in 05333, one `clickhouse-local` listens on HTTP and sends itself the queries of the other user,
# so that both borrow the one worker of the pool.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./shm_udf_scripts/common.sh
. "$CUR_DIR"/shm_udf_scripts/common.sh

# 32 MiB: far more than what the query allocates on its own, so that a limit of one and a half
# regions is fitted by the region once and not by it twice.
REGION=33554432
LIMIT=50331648

{
    echo "<function><type>executable_pool</type><name>shm_peek</name><return_type>String</return_type>"
    echo "<argument><type>UInt64</type></argument><format>TabSeparated</format><pool_size>1</pool_size>"
    echo "<use_shared_memory>1</use_shared_memory><shared_memory_size>$REGION</shared_memory_size>"
    echo "<command>shm_udf_region.py --peek</command></function>"
} | shm_functions

SCRUBBED="SELECT sum(value) FROM system.events WHERE event = 'ExecutableUDFSharedMemoryScrubbedBytes';"

echo "--- another user borrows the region under a limit it fits once"
shm_local_listening "
    SELECT shm_peek(1);
    $(shm_as_user other "SELECT shm_peek(1) SETTINGS max_memory_usage = $LIMIT, max_untracked_memory = 0")
    $SCRUBBED
"
