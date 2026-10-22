#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Tag no-replicated-database: the test creates its own databases, the Replicated ones are in 05343.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE refuses, before changing anything, a rename whose references it cannot rewrite.
# The references it rewrites are in 05342.

EXT="${CLICKHOUSE_DATABASE}_ext"

function occurrences()
{
    local name=$1
    shift
    local query=""
    for table in "$@"; do
        query+="SHOW CREATE TABLE ${table};"
    done
    ${CLICKHOUSE_CLIENT} -q "$query" | grep -o -F "$name" | wc -l
}

function definitions()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT create_table_query FROM system.tables WHERE database IN ('$1', '$2') ORDER BY database, name"
}

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${EXT} ENGINE = Atomic;
    CREATE TABLE ${EXT}.s (a UInt64) ENGINE = MergeTree ORDER BY a;
"

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
    CREATE VIEW ${R}.expression_header_v AS SELECT abs(${R}.events.x) FROM ${EXT}.events CROSS JOIN ${R}.events;
    CREATE VIEW ${R}.header_v AS SELECT * FROM ${EXT}.events JOIN ${R}.events ON ${R}.events.x = ${EXT}.events.x;
    CREATE VIEW ${R}.subquery_v AS SELECT ${R}.t.a FROM ${R}.t, (SELECT 1 AS z) AS sq;
    CREATE TABLE ${R}.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
    CREATE VIEW ${R}.in_header_v AS SELECT 1 IN ${R}.t;
    CREATE TABLE ${R}.\`t)\` (a UInt64) ENGINE = Memory;
    CREATE VIEW ${R}.in_paren_header_v AS SELECT 1 IN ${R}.\`t)\`;
    CREATE VIEW ${R}.joinget_header_v AS SELECT joinGet('${R}.j', 'v', toUInt64(1));
"
for view in alias_v dotted_alias_v expression_header_v header_v in_header_v in_paren_header_v joinget_header_v subquery_v; do
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
echo "occurrences of the old name: $(occurrences "${DICT_A}" ${DICT_B}.cv ${DICT_B}.dict_v)"

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

echo "-- table function arguments that the rename cannot rewrite"
function refused_by()
{
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "$1" 2>&1 >/dev/null \
        | sed -n "s/.*because ext\.\([a-z_]*\) refers to it in the table function .*(\([A-Z_]*\)).*/\1: \2/p"
}
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "
    CREATE DATABASE f ENGINE = Atomic;
    CREATE DATABASE ext ENGINE = Atomic;
    CREATE TABLE f.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO f.t VALUES (1);
    CREATE VIEW f.p AS SELECT {n:UInt64} AS x;
    CREATE VIEW ext.index_v AS SELECT x FROM loop(mergeTreeIndex(f, t)) LIMIT 1;
    CREATE VIEW ext.loop_v AS SELECT * FROM loop('f', 't') LIMIT 1;
    CREATE VIEW ext.qualified_loop_v AS SELECT * FROM loop(f.t) LIMIT 1;
    CREATE VIEW ext.param_v AS SELECT * FROM f.p(n = 1);
    CREATE VIEW ext.file_v AS SELECT * FROM file('f', 'CSV', 'x UInt64');
"
refused_by "RENAME DATABASE f TO g"
refused_by "DROP VIEW ext.index_v; RENAME DATABASE f TO g"
refused_by "DROP VIEW ext.loop_v; RENAME DATABASE f TO g"
refused_by "DROP VIEW ext.qualified_loop_v; RENAME DATABASE f TO \`g.dot\`"
${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" -m -q "SELECT * FROM ext.param_v; RENAME DATABASE f TO g; SELECT * FROM ext.param_v"
rm -rf "${LOCAL_DIR}"

query=""
for db in ${DICT_B} ${EXT} ${RN} ${C0} ${GB} ${TB} ${TM}; do
    query+="DROP DATABASE IF EXISTS ${db} SYNC;"
done
${CLICKHOUSE_CLIENT} -q "$query"
