#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Tag no-replicated-database: the test creates its own databases, the Replicated ones are in 05343.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE rewrites the references to the renamed database stored in the views, materialized views
# and TimeSeries tables of every database, or refuses the rename before changing anything.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
EXT="${CLICKHOUSE_DATABASE}_ext"
MEM="${CLICKHOUSE_DATABASE}_mem"
DICTDB="${CLICKHOUSE_DATABASE}_dictdb"

function rows()
{
    echo "$1: $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $1")"
}

function occurrences()
{
    ${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE $1" | grep -o -F "$2" | wc -l
}

function definitions()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT create_table_query FROM system.tables WHERE database IN ('$1', '$2') ORDER BY database, name"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${SRC} ENGINE = Atomic;
    CREATE DATABASE ${EXT} ENGINE = Atomic;
    CREATE DATABASE ${MEM} ENGINE = Memory;
    CREATE DATABASE ${DICTDB} ENGINE = Dictionary;

    CREATE TABLE ${SRC}.sample (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE ${SRC}.other (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE ${SRC}.excluded (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${SRC}.other SELECT number, toString(number) FROM numbers(3);
    INSERT INTO ${SRC}.excluded VALUES (100);

    CREATE MATERIALIZED VIEW ${SRC}.inline_mv ENGINE = MergeTree ORDER BY a AS SELECT a FROM ${SRC}.sample;
    CREATE TABLE ${SRC}.tgt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${SRC}.to_mv TO ${SRC}.tgt AS SELECT a FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.pv AS SELECT a FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.not_in_v AS SELECT a FROM ${SRC}.sample WHERE a NOT IN ${SRC}.excluded;

    CREATE TABLE ${SRC}.tgt2 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${SRC}.qualified_mv TO ${SRC}.tgt2 AS SELECT ${SRC}.sample.a AS a FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.qualified_v AS SELECT ${SRC}.sample.* FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.regexp_v AS SELECT ${SRC}.sample.COLUMNS('^a$') FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.list_v AS SELECT ${SRC}.sample.COLUMNS(a) FROM ${SRC}.sample;
    CREATE VIEW ${SRC}.join_v AS SELECT ${SRC}.sample.a AS a, b FROM ${SRC}.sample JOIN ${SRC}.other ON ${SRC}.sample.a = ${SRC}.other.a;

    CREATE TABLE ${EXT}.s (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE ${EXT}.tgt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.mv TO ${EXT}.tgt AS SELECT a FROM ${SRC}.sample;
    CREATE TABLE ${SRC}.tgt3 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.mv2 TO ${SRC}.tgt3 AS SELECT a FROM ${EXT}.s;
    CREATE VIEW ${EXT}.v AS SELECT a FROM ${SRC}.sample;
    CREATE VIEW ${MEM}.v AS SELECT a FROM ${SRC}.sample;

    CREATE MATERIALIZED VIEW ${SRC}.dep REFRESH EVERY 1 YEAR ENGINE = Memory EMPTY AS SELECT 1 AS x;
    CREATE TABLE ${SRC}.rt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${SRC}.rmv REFRESH EVERY 1 YEAR DEPENDS ON ${SRC}.dep TO ${SRC}.rt EMPTY AS SELECT a FROM ${SRC}.sample;
    CREATE TABLE ${EXT}.rt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.rmv REFRESH EVERY 1 YEAR DEPENDS ON ${SRC}.dep TO ${EXT}.rt EMPTY AS SELECT a FROM ${EXT}.s;

    CREATE VIEW ${SRC}.unrelated_v AS SELECT a FROM ${EXT}.s;
    CREATE VIEW ${EXT}.unrelated_v AS SELECT a FROM ${EXT}.s WHERE '${SRC}.sample' IN '${SRC}.sample';
"

# TimeSeries targets, adapted from #110982.
for suffix in "" 2; do
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE ${SRC}.samples${suffix} (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
        CREATE TABLE ${SRC}.tags${suffix} (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String),
            min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY id;
        CREATE TABLE ${SRC}.families${suffix} (metric_family String, type String, unit String, help String)
            ENGINE = ReplacingMergeTree ORDER BY metric_family;
    "
done
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${SRC}.ts ENGINE = TimeSeries SAMPLES ${SRC}.samples TAGS ${SRC}.tags METRIC FAMILIES ${SRC}.families;
    CREATE TABLE ${EXT}.ts ENGINE = TimeSeries SAMPLES ${SRC}.samples2 TAGS ${SRC}.tags2 METRIC FAMILIES ${SRC}.families2;
"

unrelated_src_before=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${SRC}.unrelated_v" | sed "s/${SRC}/db/g")
unrelated_ext_before=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${EXT}.unrelated_v")

${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${SRC} TO ${DST}"

function check()
{
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DST}.sample SELECT number FROM numbers(5)"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${EXT}.s SELECT number FROM numbers(2)"
    ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
        INSERT INTO ${DST}.ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.0)]);
        INSERT INTO ${EXT}.ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.0)]);
    "
    for table in ${DST}.inline_mv ${DST}.tgt ${DST}.pv ${DST}.not_in_v ${DST}.tgt2 ${DST}.qualified_v ${DST}.regexp_v ${DST}.list_v ${DST}.join_v \
        ${EXT}.tgt ${DST}.tgt3 ${EXT}.v ${MEM}.v ${DST}.samples ${DST}.samples2; do
        rows "$table" | sed "s/${CLICKHOUSE_DATABASE}//"
    done
}

echo "-- after the rename"
check
total=0
for table in ${DST}.inline_mv ${DST}.to_mv ${DST}.pv ${DST}.not_in_v ${DST}.qualified_mv ${DST}.qualified_v ${DST}.regexp_v ${DST}.list_v ${DST}.join_v ${EXT}.mv ${EXT}.mv2 ${EXT}.v ${MEM}.v ${DST}.rmv ${EXT}.rmv ${DST}.ts ${EXT}.ts; do
    total=$((total + $(occurrences "$table" "${SRC}")))
done
echo "occurrences of the old name: $total"

${CLICKHOUSE_CLIENT} -q "SYSTEM REFRESH VIEW ${DST}.rmv; SYSTEM WAIT VIEW ${DST}.rmv; SYSTEM REFRESH VIEW ${EXT}.rmv; SYSTEM WAIT VIEW ${EXT}.rmv"
rows "${DST}.rt" | sed "s/${CLICKHOUSE_DATABASE}//"
rows "${EXT}.rt" | sed "s/${CLICKHOUSE_DATABASE}//"

[ "$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DST}.unrelated_v" | sed "s/${DST}/db/g")" = "${unrelated_src_before}" ] && echo "unchanged: ${DST}.unrelated_v" | sed "s/${CLICKHOUSE_DATABASE}//"
[ "$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${EXT}.unrelated_v")" = "${unrelated_ext_before}" ] && echo "unchanged: ${EXT}.unrelated_v" | sed "s/${CLICKHOUSE_DATABASE}//"

echo "-- after a reload"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DST}; ATTACH DATABASE ${DST}; DETACH DATABASE ${EXT}; ATTACH DATABASE ${EXT}"
check
${CLICKHOUSE_CLIENT} -q "
    SELECT arraySort(arrayMap((d, t) -> concat(replaceOne(d, '${CLICKHOUSE_DATABASE}', ''), '.', t), dependencies_database, dependencies_table))
    FROM system.tables WHERE database = '${DST}' AND name = 'sample'"

echo "-- the qualified names that the rename cannot rewrite unambiguously"
R="${CLICKHOUSE_DATABASE}_ra"
RN="${CLICKHOUSE_DATABASE}_rb"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${R} ENGINE = Atomic;
    CREATE TABLE ${R}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE ${R}.events (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${EXT}.events (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW ${R}.alias_v AS SELECT ${R}.t.a, 1 AS ${RN} FROM ${R}.t;
    CREATE VIEW ${R}.dotted_alias_v AS SELECT ${R}.t.a FROM ${R}.t WHERE a IN (SELECT 1 AS \`${R}.t.a\`);
    CREATE VIEW ${R}.header_v AS SELECT * FROM ${EXT}.events JOIN ${R}.events ON ${R}.events.x = ${EXT}.events.x;
    CREATE VIEW ${R}.subquery_v AS SELECT ${R}.t.a FROM ${R}.t, (SELECT 1 AS z) AS sq;
"
for view in alias_v dotted_alias_v header_v subquery_v; do
    before=$(definitions "${R}" "${EXT}")
    ${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${R} TO ${RN}" 2>&1 | sed -n "s/.*the names in ${R}\.\([a-z_]*\) refer.*(\([A-Z_]*\)).*/\1: \2/p" | head -1
    [ "$(definitions "${R}" "${EXT}")" = "$before" ] && echo "nothing changed"
    ${CLICKHOUSE_CLIENT} -q "DROP VIEW ${R}.${view}"
done
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${R} TO ${RN}"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${RN}"

C0="${CLICKHOUSE_DATABASE}_c0"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${C0} ENGINE = Atomic;
    CREATE TABLE ${C0}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE VIEW ${C0}.qualified_v AS SELECT ${C0}.t.a FROM ${C0}.t;
"
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${C0} TO \`${C0}.x\`" 2>&1 | grep -o -m1 "a database name contains a dot"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${C0}"

echo "-- a table and a column named like the database, with no qualified name to rewrite"
GA="${CLICKHOUSE_DATABASE}_ga"
GB="${CLICKHOUSE_DATABASE}_gb"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${GA} ENGINE = Atomic;
    CREATE TABLE ${GA}.\`${GA}\` (\`${GA}\` UInt64) ENGINE = MergeTree ORDER BY \`${GA}\`;
    INSERT INTO ${GA}.\`${GA}\` VALUES (7);
    CREATE VIEW ${GA}.v AS SELECT \`${GA}\` FROM ${GA}.\`${GA}\`;
    RENAME DATABASE ${GA} TO ${GB};
    SELECT * FROM ${GB}.v;
"

echo "-- column expressions and the dictionary() table function"
DICT_A="${CLICKHOUSE_DATABASE}_dicta"
DICT_B="${CLICKHOUSE_DATABASE}_dictb"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${EXT}.dict_source (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO ${EXT}.dict_source VALUES (1, 'one');
    CREATE DATABASE ${DICT_A} ENGINE = Atomic;
    CREATE DICTIONARY ${DICT_A}.dict (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dict_source' DB '${EXT}')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE TABLE ${DICT_A}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${DICT_A}.t VALUES (1);
    CREATE VIEW ${DICT_A}.cv (a UInt64, d String ALIAS dictGet('${DICT_A}.dict', 'v', a)) AS SELECT a FROM ${DICT_A}.t;
    CREATE VIEW ${DICT_A}.dict_v AS SELECT v FROM dictionary('${DICT_A}.dict');
"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW ${DICT_A}.inner_mv (a UInt64, d String DEFAULT dictGet('${DICT_A}.dict', 'v', a)) ENGINE = MergeTree ORDER BY a AS SELECT a FROM ${DICT_A}.t"
${CLICKHOUSE_CLIENT} --check_table_dependencies=0 -q "RENAME DATABASE ${DICT_A} TO ${DICT_B}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DICT_A}"
INNER=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DICT_A}' AND name LIKE '.inner_id.%'")
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${DICT_A}.\`${INNER}\` PERMANENTLY"
${CLICKHOUSE_CLIENT} --check_table_dependencies=0 -q "RENAME DATABASE ${DICT_A} TO ${DICT_B}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DICT_A}"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${DICT_A}.\`${INNER}\`"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DICT_A}.inner_mv SYNC"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "CREATE TABLE ${DICT_A}.inner_ts ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64 DEFAULT toUInt64OrZero(dictGet('${DICT_A}.dict', 'v', sipHash64(metric_name))))"
${CLICKHOUSE_CLIENT} --check_table_dependencies=0 -q "RENAME DATABASE ${DICT_A} TO ${DICT_B}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${DICT_A}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${DICT_A}.inner_ts SYNC"
# A TimeSeries table of a newer version must not be altered.
uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DICT_A}.samples (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp)"
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_experimental_time_series_table=1 -q "ATTACH TABLE ${EXT}.ts_future UUID '${uuid}' (metric_name String) ENGINE = TimeSeries SETTINGS version = 999 SAMPLES ${DICT_A}.samples"
${CLICKHOUSE_CLIENT} --check_table_dependencies=0 -q "RENAME DATABASE ${DICT_A} TO ${DICT_B}" 2>&1 | grep -o -m1 "INCOMPATIBLE_SCHEMA"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${EXT}.ts_future SYNC"
# The dictionary has dependents, which the default check refuses to rename.
${CLICKHOUSE_CLIENT} --check_table_dependencies=0 -q "RENAME DATABASE ${DICT_A} TO ${DICT_B}"
${CLICKHOUSE_CLIENT} -q "SELECT d FROM ${DICT_B}.cv; SELECT v FROM ${DICT_B}.dict_v"
echo "occurrences of the old name: $(( $(occurrences "${DICT_B}.cv" "${DICT_A}") + $(occurrences "${DICT_B}.dict_v" "${DICT_A}") ))"

echo "-- a detached view"
TA="${CLICKHOUSE_DATABASE}_ta"
TB="${CLICKHOUSE_DATABASE}_tb"
TM="${CLICKHOUSE_DATABASE}_tm"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${TA} ENGINE = Atomic;
    CREATE TABLE ${TA}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${TA}.t VALUES (1);
    CREATE VIEW ${TA}.dv AS SELECT a FROM ${TA}.t;
    CREATE VIEW ${TA}.dv_unrelated AS SELECT a FROM ${EXT}.s;
    DETACH VIEW ${TA}.dv;
    DETACH VIEW ${TA}.dv_unrelated;
"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${TM} ENGINE = Memory;
    CREATE VIEW ${TM}.dv AS SELECT a FROM ${TA}.t;
    DETACH VIEW ${TM}.dv;
"
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${TA} TO ${TB}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${TA}"
# Only the view of the Memory database is detached now.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${TA}.dv"
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${TA} TO ${TB}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} -q "EXISTS DATABASE ${TA}"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${TM}.dv; RENAME DATABASE ${TA} TO ${TB}; SELECT count() FROM ${TB}.dv; SELECT count() FROM ${TM}.dv"

echo "-- clickhouse-local, a view in the default (Overlay) database"
LOCAL_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
rm -rf "${LOCAL_DIR}"
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "
    CREATE DATABASE a ENGINE = Atomic;
    CREATE TABLE a.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE default.d (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW default.v AS SELECT x FROM a.t;
    CREATE MATERIALIZED VIEW default.mv TO default.d AS SELECT x FROM a.t;
    RENAME DATABASE a TO b;
    INSERT INTO b.t VALUES (1);
    SELECT count() FROM default.v;
    SELECT count() FROM default.d;
"
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "
    INSERT INTO b.t VALUES (2);
    SELECT count() FROM default.v;
    SELECT count() FROM default.d;
    SELECT countIf(create_table_query LIKE '% a.%') FROM system.tables WHERE database = 'default' AND name IN ('v', 'mv');
"
rm -rf "${LOCAL_DIR}"

for db in ${DICT_B} ${DST} ${EXT} ${MEM} ${DICTDB} ${RN} ${C0} ${GB} ${TB} ${TM}; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
done
