#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the H3 library is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `h3kRing` expands a whole block of rows inside one `IFunction::executeImpl` call, so the executor's
# between-blocks cancellation check could not bound it and `max_execution_time` was ignored until the last
# row was done. The cost of a row is `maxGridDiskSize(k)` regardless of how many cells actually exist at the
# cell's resolution, so a coarse cell with a large `k` burns minutes per block while producing almost
# nothing: the query below returns 122 cells per row and does not run out of memory, it just runs.
#
# The oracle is the outer `timeout`, not a measured elapsed time. With the deadline observed this stops in
# about a second; without it the same query took 250 s on a release build and 993 s under MSan in CI.

if timeout 60 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --max_block_size 65409 --max_rows_to_read 0 \
        --max_memory_usage 0 --query "
            SELECT sum(length(h3kRing(materialize(579205133326352383), toUInt16(1023)))) FROM numbers(65409)
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
then
    echo 'stopped at the deadline'
else
    echo 'still running after 60 seconds'
fi

# The checkpoint must not change what the function returns. 579205133326352383 is a resolution 0 cell, so
# every disk larger than the grid is the whole grid; 644325529233966508 is resolution 15, where the disk is
# the full 3 * k * (k + 1) + 1 cells.

${CLICKHOUSE_CLIENT} --query "
    SELECT
        length(h3kRing(materialize(579205133326352383), toUInt16(1023))),
        length(h3kRing(materialize(644325529233966508), toUInt16(100))),
        arraySort(h3kRing(materialize(579205133326352383), toUInt16(1))) = arraySort(h3kRing(579205133326352383, toUInt16(1)))
"
