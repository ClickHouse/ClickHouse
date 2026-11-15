#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the H3 library is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A needle-thin polygon whose bounding box covers much of the globe. The search descends into every cell of
# that box and the needle contains the center of almost none of them, so a checkpoint per returned cell
# observes nothing and the query used to run for minutes past the deadline. The oracle is the outer
# `timeout`, whose margin holds on a sanitizer build because the unchecked version grows with it too.

NEEDLE="[(-100., -60.), (100., 60.), (-99.9999999, -60.)]"

if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --query "
            SELECT length(h3PolygonToCells(${NEEDLE}, 8))
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
then
    echo 'stopped at the deadline'
else
    echo 'still running after 30 seconds'
fi

# The same shape with a containment mode, where a candidate cell is also tested for overlap.

if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --query "
            SELECT length(h3PolygonToCellsWithContainment(${NEEDLE}, 8, 1))
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
then
    echo 'stopped at the deadline'
else
    echo 'still running after 30 seconds'
fi
