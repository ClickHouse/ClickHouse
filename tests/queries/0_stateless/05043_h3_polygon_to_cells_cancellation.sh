#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the H3 library is not built in fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every candidate cell is tested against every vertex, so the comb below - 20000 valid vertices inside half
# a degree - took 29 s in a single row, with nothing observing the deadline until the row was done. The
# oracle is the outer `timeout`: a `TIMEOUT_EXCEEDED` arriving 29 s after a 1 s deadline still fails, and
# the margin holds on a sanitizer build because the unbounded version grows with it.

COMB="arrayConcat([(0., 0.)], arrayMap(i -> (0.5 + 0.4 * (i % 2), i * 0.00002), range(20000)), [(0., 0.4)])"

if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --query "
            SELECT length(h3PolygonToCells(${COMB}, 10))
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
then
    echo 'stopped at the deadline'
else
    echo 'still running after 30 seconds'
fi

# A vertex off the sphere is rejected instead of reaching H3, which derives a meaningless bounding box.

${CLICKHOUSE_CLIENT} --query "
        SELECT length(h3PolygonToCells([(-122.40898669999721, -1.), (2., 100.0001), (-1., -1.7976931348623157e308)], 9))
    " 2>&1 | grep -o -m 1 -F 'out of bounds (longitude must be -180..180 and latitude -90..90 degrees)'

# A NaN vertex never reaches that check: converting the geometry rejects it first, together with infinity.

${CLICKHOUSE_CLIENT} --query "SELECT length(h3PolygonToCells([(nan, 0.), (1., 0.), (1., 1.)], 9))" 2>&1 |
    grep -o -m 1 -F "Point's component must not be NaN"

${CLICKHOUSE_CLIENT} --query "SELECT length(h3PolygonToCells([(inf, 0.), (1., 0.), (1., 1.)], 9))" 2>&1 |
    grep -o -m 1 -F "Point's component must not be infinite"

# The cells covering a geometry are those whose center it contains, which is containment mode 0, so the two
# functions agree.

${CLICKHOUSE_CLIENT} --query "
    WITH
        [(-122.4089866999972145, 37.813318999983238), (-122.3544736999993603, 37.7198061999978478), (-122.4798767000009008, 37.8151571999998453)] AS sf,
        [(55.66824, 12.595493), (55.667901, 12.593991), (55.667474, 12.595117), (55.66824, 12.595493)] AS cph
    SELECT
        resolution,
        arraySort(h3PolygonToCells(sf, resolution)) = arraySort(h3PolygonToCellsWithContainment(sf, resolution, 0)),
        arraySort(h3PolygonToCells(cph, resolution)) = arraySort(h3PolygonToCellsWithContainment(cph, resolution, 0))
    FROM (SELECT arrayJoin([toUInt8(5), 7, 9, 11]) AS resolution)
    ORDER BY resolution
"
