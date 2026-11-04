#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the H3 library is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertex off the sphere used to reach H3 as it came, and its size estimate then counted every cell of a
# polygon covering much of the globe: about 45 minutes in one call, observing neither the deadline nor
# `KILL QUERY`. The oracle is the outer `timeout`.

timeout 30 ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(length(h3PolygonToCellsWithContainment([(materialize(-122.40898669999721), -1.), (2., 100.0001), (-1., -1.7976931348623157e308)], 9, 0))) FROM numbers(200)
    " 2>&1 | grep -o -m 1 -F 'out of bounds (longitude must be -180..180 and latitude -90..90 degrees)'

# Valid vertices can still ask for more work than the deadline allows: every candidate cell is tested
# against every vertex, so the comb below - 20000 vertices inside half a degree - costs 7.5 s in a single
# row. The cells are now enumerated one at a time, so the deadline is observed mid-row. The oracle is the
# outer `timeout`, which is why the query must not depend on how fast the build is.

COMB="arrayConcat([(0., 0.)], arrayMap(i -> (0.5 + 0.4 * (i % 2), i * 0.00002), range(20000)), [(0., 0.4)])"

if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --query "
            SELECT length(h3PolygonToCellsWithContainment(${COMB}, 10, 0))
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
then
    echo 'stopped at the deadline'
else
    echo 'still running after 30 seconds'
fi

# The checkpoint must not change what the function returns.

${CLICKHOUSE_CLIENT} --query "
    SELECT
        length(h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 9, 0)),
        arraySort(h3PolygonToCellsWithContainment([(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 2))
        = arraySort(h3PolygonToCellsWithContainment([(materialize(-122.4089866999972145), 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)], 7, 2))
"
