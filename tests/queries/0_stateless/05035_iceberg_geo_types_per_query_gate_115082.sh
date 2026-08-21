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
PLAIN="p_${CLICKHOUSE_DATABASE}_${RANDOM}"
PLAIN_PATH="${USER_FILES_PATH}/${PLAIN}/"
MERGE_GEO="mg_${CLICKHOUSE_DATABASE}_${RANDOM}"
MERGE_PLAIN="mp_${CLICKHOUSE_DATABASE}_${RANDOM}"

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

# Reading through Merge is gated too. Merge does not resolve its children's dynamic metadata, so
# these arms cover the read paths that never see the per-query check applied on a direct read: the
# trivial count() optimization, and a scan of a snapshot that an earlier query already pinned.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${MERGE_GEO}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${MERGE_GEO} ENGINE = Merge(currentDatabase(), '^${TABLE}\$')"

# Cold: no query has read this table since it was attached.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# A query that enables the flag reads through Merge, and pins the table state. Asserted as "not
# refused" rather than as a row count: the trivial count() of a just-written Iceberg table read
# through Merge races with snapshot visibility and returns 0 instead of 1 in about 6 runs in 100,
# equally on master. What this test is about is who is allowed to read, so it asserts exactly that.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT count() FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT id, wkt(g) FROM ${MERGE_GEO}"

# Warm: the same two flag-less reads must still be refused. Enforcement that depends on whether an
# earlier query happened to warm the state is the defect this test guards against.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# system.tables reads total_rows of every table, and must not be broken by a refused one.
${CLICKHOUSE_CLIENT} --query "SELECT count() >= 2 FROM system.tables WHERE database = currentDatabase()"

# A table without a geometry column is unaffected on each of the three paths gated above: the
# trivial count() through Merge, a direct read, and a scan through Merge of a pinned snapshot.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PLAIN}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${PLAIN} (id Int64, s String)
    ENGINE = IcebergLocal('${PLAIN_PATH}', 'Parquet')
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${PLAIN} SELECT 7, 'x'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${MERGE_PLAIN}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${MERGE_PLAIN} ENGINE = Merge(currentDatabase(), '^${PLAIN}\$')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${MERGE_PLAIN}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "SELECT id, s FROM ${PLAIN}"
${CLICKHOUSE_CLIENT} --query "SELECT id, s FROM ${MERGE_PLAIN}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${MERGE_GEO}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${MERGE_PLAIN}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${NESTED}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PLAIN}"
rm -rf "${TABLE_PATH}" "${NESTED_PATH}" "${PLAIN_PATH}"
