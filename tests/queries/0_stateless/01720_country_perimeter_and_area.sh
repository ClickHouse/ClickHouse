#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists country_polygons;"
${CLICKHOUSE_CLIENT} -q "create table country_polygons(name String, p Array(Array(Tuple(Float64, Float64)))) engine=MergeTree() order by tuple();"
cat ${CURDIR}/country_polygons.tsv | ${CLICKHOUSE_CLIENT} -q "insert into country_polygons format TSV"

${CLICKHOUSE_CLIENT} -q "SELECT name, round(polygonPerimeterSpherical(p), 6) from country_polygons"
${CLICKHOUSE_CLIENT} -q "SELECT '-------------------------------------'"
${CLICKHOUSE_CLIENT} -q "SELECT name, round(polygonAreaSpherical(p), 6) from country_polygons"
${CLICKHOUSE_CLIENT} -q "SELECT '-------------------------------------'"
${CLICKHOUSE_CLIENT} -q "drop table if exists country_rings;"


${CLICKHOUSE_CLIENT} -q "create table country_rings(name String, p Array(Tuple(Float64, Float64))) engine=MergeTree() order by tuple();"
cat ${CURDIR}/country_rings.tsv | ${CLICKHOUSE_CLIENT} -q "insert into country_rings format TSV"

${CLICKHOUSE_CLIENT} -q "SELECT name, round(polygonPerimeterSpherical(p), 6) from country_rings"
${CLICKHOUSE_CLIENT} -q "SELECT '-------------------------------------'"
${CLICKHOUSE_CLIENT} -q "SELECT name, round(polygonAreaSpherical(p), 6) from country_rings"
${CLICKHOUSE_CLIENT} -q "SELECT '-------------------------------------'"
${CLICKHOUSE_CLIENT} -q "drop table if exists country_rings;"

${CLICKHOUSE_CLIENT} -q "drop table country_polygons"
