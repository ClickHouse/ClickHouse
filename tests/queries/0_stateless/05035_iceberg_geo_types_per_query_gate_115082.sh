#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/115082

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
NESTED="n_${CLICKHOUSE_DATABASE}_${RANDOM}"
NESTED_PATH="${USER_FILES_PATH}/${NESTED}/"

GEO_REFUSED="allow_experimental_geo_types_in_iceberg"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "
    CREATE TABLE ${TABLE} (id Int64, g Geometry)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_experimental_geo_types_in_iceberg=1 \
    --query "INSERT INTO ${TABLE} SELECT 1, readWKT('POINT(1 2)')"

# A query that enables the flag reads the geometry column.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT id, wkt(g) FROM ${TABLE}"

# A query that does not enable the flag is refused, even though the table was created by a query
# that did enable it.
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${TABLE}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# DETACH + ATTACH without the flag simulates a server restart, which reloads the table with the
# server default settings. Both statements must succeed.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${TABLE}"

# The reloaded table is still readable by a query that enables the flag.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT id, wkt(g) FROM ${TABLE}"

# And still refused without it.
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${TABLE}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# The table function is gated by the invoking query too.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 \
    --query "SELECT id, wkt(g) FROM icebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM icebergLocal('${TABLE_PATH}', 'Parquet')" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# A geometry nested inside a Tuple is gated the same way.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NESTED}"
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "
    CREATE TABLE ${NESTED} (id Int64, t Tuple(a Int64, g Geometry))
    ENGINE = IcebergLocal('${NESTED_PATH}', 'Parquet')
"
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT count() FROM ${NESTED}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${NESTED}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NESTED}"
rm -rf "${TABLE_PATH}" "${NESTED_PATH}"
