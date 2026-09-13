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
INFERRED="i_${CLICKHOUSE_DATABASE}_${RANDOM}"
COMPACT="c_${CLICKHOUSE_DATABASE}_${RANDOM}"
COMPACT_PATH="${USER_FILES_PATH}/${COMPACT}/"

GEO_REFUSED="allow_experimental_geo_types_in_iceberg"

# Setup statements go through one client session per group, carrying the union of the settings the
# group needs. Only statements that must all succeed may be grouped: an exception skips the rest of
# its session. Every arm that asserts stays its own session, so that it carries exactly the settings
# it is about.
setup() {
    ${CLICKHOUSE_CLIENT} "$@"
}

# An arm whose passing answer is "not refused" needs the query's exit status as well as the error
# token: a token-presence test alone reports an unrelated failure as a permitted read.
run_allowed() {
    local out rc
    out=$(${CLICKHOUSE_CLIENT} "$@" 2>&1)
    rc=$?
    if echo "$out" | grep -qF "${GEO_REFUSED}"; then echo REFUSED
    elif [ $rc -eq 0 ]; then echo NOT_REFUSED
    else echo "QUERY_FAILED: $out"; fi
}

setup --allow_experimental_geo_types_in_iceberg=1 --allow_insert_into_iceberg=1 --query "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (id Int64, g Geometry)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet');
    INSERT INTO ${TABLE} SELECT 1, readWKT('POINT(1 2)');
"

# A query that enables the flag reads the geometry column.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT id, wkt(g) FROM ${TABLE}"

# A query that does not enable the flag is refused, even though the table was created by a query
# that did enable it.
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${TABLE}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# DETACH + ATTACH without the flag simulates a server restart, which reloads the table with the
# server default settings. All three statements must succeed.
setup --query "DETACH TABLE ${TABLE}; ATTACH TABLE ${TABLE}; EXISTS TABLE ${TABLE};"

# The reloaded table is still readable by a query that enables the flag.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT id, wkt(g) FROM ${TABLE}"

# And still refused without it.
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${TABLE}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# The table function is gated by the invoking query too.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 \
    --query "SELECT id, wkt(g) FROM icebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM icebergLocal('${TABLE_PATH}', 'Parquet')" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# Handing the schema to a caller is gated on its own, with no column being read: inferring a
# structure from the metadata exposes the geometry field's type. These arms read no data, so the
# paths that gate a scan and a row count cannot be what answers them.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 \
    --query "DESCRIBE icebergLocal('${TABLE_PATH}', 'Parquet')" | grep -c Geometry
${CLICKHOUSE_CLIENT} --query "DESCRIBE icebergLocal('${TABLE_PATH}', 'Parquet')" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${INFERRED}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${INFERRED} ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
# Dropped between the two arms: without the setting the create above is refused and creates nothing,
# but a build that permits it would leave the table behind and make this arm fail on the name.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${INFERRED}"
run_allowed --allow_experimental_geo_types_in_iceberg=1 --query "CREATE TABLE ${INFERRED} ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

# Reading the columns already stored for an attached table is not gated, and every entrypoint that
# does so agrees. One of these refreshes the in-memory metadata first and the others read it as it
# stands, so gating the refresh would make the same table visible or not according to which
# statement the user picked.
run_allowed --query "DESCRIBE TABLE ${TABLE}"
run_allowed --query "SHOW COLUMNS FROM ${TABLE}"
run_allowed --query "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '${TABLE}'"

# A geometry nested inside a Tuple is gated the same way.
setup --allow_experimental_geo_types_in_iceberg=1 --query "
    DROP TABLE IF EXISTS ${NESTED};
    CREATE TABLE ${NESTED} (id Int64, t Tuple(a Int64, g Geometry))
    ENGINE = IcebergLocal('${NESTED_PATH}', 'Parquet');
"
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT count() FROM ${NESTED}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${NESTED}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# Reading through Merge is gated too. Merge does not resolve its children's dynamic metadata, so
# these arms cover the read paths that never see the per-query check applied on a direct read: the
# trivial count() optimization, and a scan of a snapshot that an earlier query already pinned.
#
# The DETACH + ATTACH leaves the table cold: no query has read it since it was attached, and no
# statement in this session reads it either. The count() arms pin optimize_trivial_count_query: with
# it off the count runs as an ordinary scan and is answered by the other gated path, so the arm
# would no longer be about the trivial-count path it is here to cover.
setup --query "
    DROP TABLE IF EXISTS ${MERGE_GEO};
    CREATE TABLE ${MERGE_GEO} ENGINE = Merge(currentDatabase(), '^${TABLE}\$');
    DETACH TABLE ${TABLE};
    ATTACH TABLE ${TABLE};
"
${CLICKHOUSE_CLIENT} --optimize_trivial_count_query=1 --query "SELECT count() FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# A query that enables the flag reads through Merge, and pins the table state. Asserted as "not
# refused" rather than as a row count: the trivial count() of a just-written Iceberg table read
# through Merge races with snapshot visibility and returns 0 instead of 1 in about 6 runs in 100,
# equally on master. What this test is about is who is allowed to read, so it asserts exactly that.
run_allowed --optimize_trivial_count_query=1 --allow_experimental_geo_types_in_iceberg=1 --query "SELECT count() FROM ${MERGE_GEO}"
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "
    SELECT id, wkt(g) FROM ${MERGE_GEO};
    SELECT id, wkt(g) FROM ${TABLE};
"

# Warm: the same two flag-less reads must still be refused. Enforcement that depends on whether an
# earlier query happened to warm the state is the defect this test guards against.
${CLICKHOUSE_CLIENT} --optimize_trivial_count_query=1 --query "SELECT count() FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --query "SELECT id, wkt(g) FROM ${MERGE_GEO}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# system.tables answers total_rows for a table it may not read as NULL rather than failing the whole
# query. The column has to be named for the row count to be reached at all. Reaching it means
# system.tables logs the refusal it swallows, so silence the log channel the client forwards to its
# stderr, as the adjacent Iceberg tests do.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --query "SELECT total_rows IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"

# A table without a geometry column is unaffected on each of the three paths gated above: the
# trivial count() through Merge, a direct read, and a scan through Merge of a pinned snapshot.
setup --allow_insert_into_iceberg=1 --query "
    DROP TABLE IF EXISTS ${PLAIN};
    CREATE TABLE ${PLAIN} (id Int64, s String)
    ENGINE = IcebergLocal('${PLAIN_PATH}', 'Parquet');
    INSERT INTO ${PLAIN} SELECT 7, 'x';
    DROP TABLE IF EXISTS ${MERGE_PLAIN};
    CREATE TABLE ${MERGE_PLAIN} ENGINE = Merge(currentDatabase(), '^${PLAIN}\$');
"
run_allowed --optimize_trivial_count_query=1 --query "SELECT count() FROM ${MERGE_PLAIN}"
${CLICKHOUSE_CLIENT} --query "
    SELECT id, s FROM ${PLAIN};
    SELECT id, s FROM ${MERGE_PLAIN};
"

# And its total_rows is a number, so the NULL asserted above is this table being unreadable by that
# query rather than how system.tables always answers for Iceberg.
${CLICKHOUSE_CLIENT} --query "SELECT total_rows IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${PLAIN}'"

# Compaction reads the data files through the table's columns and writes them back, so both OPTIMIZE
# entrypoints are gated on those columns. Every arm below enables the compaction setting, so what is
# measured is the geo gate rather than the refusal that guards compaction itself. The row delete is
# what makes the rewrite happen at all: compaction only rewrites data files when the table has a
# position delete file, so without it these arms would run the planner and rewrite nothing.
setup --allow_experimental_geo_types_in_iceberg=1 --allow_insert_into_iceberg=1 --mutations_sync=2 --query "
    DROP TABLE IF EXISTS ${COMPACT};
    CREATE TABLE ${COMPACT} (id Int64, g Geometry)
    ENGINE = IcebergLocal('${COMPACT_PATH}', 'Parquet');
    INSERT INTO ${COMPACT} SELECT number, readWKT(concat('POINT(', toString(number), ' ', toString(number * 2), ')')) FROM numbers(10, 40);
    ALTER TABLE ${COMPACT} DELETE WHERE id = 11;
"
# DETACH + ATTACH without the flag: the table is reloaded as a restart would reload it. Kept in a
# session of its own so that the reload carries the server default, which is what the arms below are
# about: a reload that saw the flag would latch the opposite answer.
setup --query "DETACH TABLE ${COMPACT}; ATTACH TABLE ${COMPACT};"
# The rewrite is armed: a table with no position delete file would report 0 here and the arms below
# would be about the planner rather than about reading the data.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_experimental_geo_types_in_iceberg=1 --query "
    SELECT count() > 0 FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = '${COMPACT}' AND content = 'POSITION_DELETE'
"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${COMPACT}" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${COMPACT} MANIFEST" 2>&1 | grep -qF "${GEO_REFUSED}" && echo REFUSED || echo NOT_REFUSED

# Controls, one per refused arm: the same two statements with the flag. Without them a build that
# refused every OPTIMIZE would pass the two arms above. Only the geo refusal is asserted against,
# because past it the cloud build reports its own user-facing exception (it routes OPTIMIZE through
# an internal flag rather than the query-level compaction setting), as the adjacent Iceberg
# compaction tests also account for.
not_geo_refused() {
    if ${CLICKHOUSE_CLIENT} "$@" 2>&1 | grep -qF "${GEO_REFUSED}"; then echo REFUSED; else echo NOT_REFUSED; fi
}
not_geo_refused --allow_experimental_iceberg_compaction=1 --allow_experimental_geo_types_in_iceberg=1 --query "OPTIMIZE TABLE ${COMPACT}"
not_geo_refused --allow_experimental_iceberg_compaction=1 --allow_experimental_geo_types_in_iceberg=1 --query "OPTIMIZE TABLE ${COMPACT} MANIFEST"
not_geo_refused --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${PLAIN}"

# The flag-enabled control above really compacted, rather than declining for some other reason:
# compaction rewrites the manifests with data files only, so the position delete file is gone. Reads
# apply that file whether or not compaction ran, so the row values alone could not tell the two
# apart. On the cloud build compaction does not run here, so there is nothing to assert about it.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo 0
else
    ${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_experimental_geo_types_in_iceberg=1 --query "
        SELECT count() FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '${COMPACT}' AND content = 'POSITION_DELETE'
    "
fi
# And the geometry values survived that rewrite.
${CLICKHOUSE_CLIENT} --allow_experimental_geo_types_in_iceberg=1 --query "SELECT count(), min(id), max(id) FROM ${COMPACT}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${COMPACT};
    DROP TABLE IF EXISTS ${MERGE_GEO};
    DROP TABLE IF EXISTS ${MERGE_PLAIN};
    DROP TABLE IF EXISTS ${TABLE};
    DROP TABLE IF EXISTS ${NESTED};
    DROP TABLE IF EXISTS ${PLAIN};
    DROP TABLE IF EXISTS ${INFERRED};
"
rm -rf "${TABLE_PATH}" "${NESTED_PATH}" "${PLAIN_PATH}" "${COMPACT_PATH}"
