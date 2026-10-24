#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A preprocessed constant polygon must take memory proportional to its 100000 vertices (16 bytes each),
# not to the vertices times the number of grid cells the boundary crosses. Every clickhouse-local starts
# with an empty cache, so the cache holds exactly the polygon of its query.

for polygon in \
    "(SELECT groupArray((cos(a), sin(a))) FROM (SELECT number * 2 * pi() / 100000 AS a FROM numbers(100000)))" \
    "(SELECT [[(-2., -2.), (2., -2.), (2., 2.), (-2., 2.)], groupArray((cos(a), sin(a)))] FROM (SELECT number * 2 * pi() / 100000 AS a FROM numbers(100000)))" \
    "(SELECT [[groupArray((cos(a) - 2, sin(a)))], [groupArray((cos(a) + 2, sin(a)))]] FROM (SELECT number * 2 * pi() / 50000 AS a FROM numbers(50000)))"
do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT pointInPolygon((0.5, 0.5), $polygon) FORMAT Null;
        SELECT value BETWEEN 1 AND 4 * 100000 * 16 FROM system.metrics WHERE metric = 'PointInPolygonCacheBytes'"
done
