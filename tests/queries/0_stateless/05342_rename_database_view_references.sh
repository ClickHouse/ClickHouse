#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Tag no-replicated-database: the test creates its own databases, the Replicated ones are in 05343.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE rewrites the references to the renamed database stored in the views, materialized views
# of every database. The renames it refuses are in 05344, the TimeSeries tables in 05345.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
EXT="${CLICKHOUSE_DATABASE}_ext"
MEM="${CLICKHOUSE_DATABASE}_mem"
FSDB="${CLICKHOUSE_DATABASE}_fs"

function rows()
{
    local query=""
    for table in "$@"; do
        query+="SELECT '${table}: ' || toString(count()) FROM ${table};"
    done
    ${CLICKHOUSE_CLIENT} -q "$query" | sed "s/${CLICKHOUSE_DATABASE}//"
}

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

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${SRC} ENGINE = Atomic;
    CREATE DATABASE ${EXT} ENGINE = Atomic;
    CREATE DATABASE ${MEM} ENGINE = Memory;
    -- A database engine without detached tables must not make the rename fail.
    CREATE DATABASE ${FSDB} ENGINE = Filesystem;

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
    CREATE VIEW ${SRC}.intersect_v AS SELECT a FROM ${SRC}.sample INTERSECT DISTINCT SELECT a FROM ${SRC}.other;
    CREATE VIEW ${SRC}.except_v AS SELECT a FROM ${SRC}.sample EXCEPT DISTINCT SELECT a FROM ${SRC}.other;
    CREATE VIEW ${SRC}.literal_v AS SELECT concat('${SRC}.sample.a', toString(a)) FROM ${SRC}.sample;
    CREATE TABLE ${SRC}.j (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
    CREATE VIEW ${EXT}.literal_arg_v AS SELECT concat('${SRC}.j', '${SRC}.excluded', toString(a)) FROM ${SRC}.sample
        WHERE a NOT IN ${SRC}.excluded AND joinGet('${SRC}.j', 'v', a) = '';
    CREATE TABLE ${SRC}.tgt4 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${SRC}.union_mv TO ${SRC}.tgt4 AS SELECT a FROM (SELECT a FROM ${SRC}.sample UNION ALL SELECT a FROM ${SRC}.sample);
    CREATE MATERIALIZED VIEW ${SRC}.union_rmv REFRESH EVERY 1 YEAR APPEND ENGINE = Memory EMPTY AS SELECT a FROM ${SRC}.sample UNION ALL SELECT a FROM ${SRC}.sample;

    CREATE TABLE ${EXT}.s (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE ${EXT}.tgt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.mv TO ${EXT}.tgt AS SELECT a FROM ${SRC}.sample;
    CREATE TABLE ${EXT}.cte_tgt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.cte_mv TO ${EXT}.cte_tgt AS WITH cte AS (SELECT a FROM ${SRC}.sample) SELECT a FROM cte;
    CREATE TABLE ${SRC}.tgt3 (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.mv2 TO ${SRC}.tgt3 AS SELECT a FROM ${EXT}.s;
    CREATE VIEW ${EXT}.v AS SELECT a FROM ${SRC}.sample;
    CREATE VIEW ${MEM}.v AS SELECT a FROM ${SRC}.sample;
    CREATE VIEW ${EXT}.merge_v AS SELECT a FROM merge('${SRC}', '^sample$');
    CREATE VIEW ${EXT}.remote_v (a UInt64) AS SELECT a FROM ${SRC}.sample
        UNION ALL SELECT a FROM remote('127.0.0.2', merge('${SRC}', '^sample$')) UNION ALL SELECT a FROM remote('127.0.0.2', ${SRC}.sample)
        UNION ALL SELECT a FROM remoteSecure('127.0.0.2', view(SELECT a FROM ${SRC}.sample))
        UNION ALL SELECT a FROM cluster('test_shard_localhost', merge('${SRC}', '^sample$'))
        UNION ALL SELECT a FROM clusterAllReplicas('test_shard_localhost', view(SELECT a FROM ${SRC}.sample));
    CREATE VIEW ${SRC}.param_v AS SELECT a FROM ${SRC}.sample WHERE a >= {n:UInt64};
    CREATE VIEW ${EXT}.param_call_v AS SELECT a FROM ${SRC}.param_v(n = 0);
    CREATE TABLE ${EXT}.prt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.param_rmv REFRESH EVERY 1 YEAR APPEND TO ${EXT}.prt EMPTY AS SELECT a FROM ${SRC}.param_v(n = 0);

    CREATE MATERIALIZED VIEW ${SRC}.dep REFRESH EVERY 1 YEAR ENGINE = Memory EMPTY AS SELECT 1 AS x;
    CREATE TABLE ${SRC}.rt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${SRC}.rmv REFRESH EVERY 1 YEAR DEPENDS ON ${SRC}.dep TO ${SRC}.rt EMPTY AS SELECT a FROM ${SRC}.sample;
    CREATE TABLE ${EXT}.rt (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW ${EXT}.rmv REFRESH EVERY 1 YEAR DEPENDS ON ${SRC}.dep TO ${EXT}.rt EMPTY AS SELECT a FROM ${EXT}.s;

    CREATE VIEW ${SRC}.unrelated_v AS SELECT a FROM ${EXT}.s;
    CREATE VIEW ${EXT}.unrelated_v AS SELECT a FROM ${EXT}.s WHERE '${SRC}.sample' IN '${SRC}.sample';
"

unrelated_src_before=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${SRC}.unrelated_v" | sed "s/${SRC}/db/g")
unrelated_ext_before=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${EXT}.unrelated_v")

# A temporary view on the server does not make the rename fail.
${CLICKHOUSE_CLIENT} -q "
    CREATE TEMPORARY VIEW tmp_v AS SELECT a FROM ${EXT}.s;
    RENAME DATABASE ${SRC} TO ${DST};
    SELECT 'tmp_v: ' || toString(count()) FROM tmp_v;
"

function check()
{
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DST}.sample SELECT number FROM numbers(5); INSERT INTO ${EXT}.s SELECT number FROM numbers(2)"
    rows ${DST}.inline_mv ${DST}.tgt ${DST}.pv ${DST}.not_in_v ${DST}.tgt2 ${DST}.qualified_v ${DST}.regexp_v ${DST}.list_v ${DST}.join_v \
        ${DST}.intersect_v ${DST}.except_v ${DST}.literal_v ${DST}.tgt4 ${EXT}.tgt ${EXT}.cte_tgt ${DST}.tgt3 ${EXT}.v ${MEM}.v ${EXT}.merge_v ${EXT}.param_call_v ${EXT}.literal_arg_v
}

echo "-- after the rename"
check
echo "occurrences of the old name: $(occurrences "${SRC}" ${DST}.inline_mv ${DST}.to_mv ${DST}.pv ${DST}.not_in_v ${DST}.qualified_mv \
    ${DST}.qualified_v ${DST}.regexp_v ${DST}.list_v ${DST}.join_v ${DST}.intersect_v ${DST}.except_v ${DST}.union_mv ${DST}.union_rmv ${EXT}.mv ${EXT}.cte_mv ${EXT}.mv2 \
    ${EXT}.v ${MEM}.v ${DST}.rmv ${EXT}.rmv ${EXT}.merge_v ${DST}.param_v ${EXT}.param_call_v ${EXT}.param_rmv)"

${CLICKHOUSE_CLIENT} -q "
    SYSTEM REFRESH VIEW ${DST}.rmv; SYSTEM WAIT VIEW ${DST}.rmv; SYSTEM REFRESH VIEW ${EXT}.rmv; SYSTEM WAIT VIEW ${EXT}.rmv;
    SYSTEM REFRESH VIEW ${DST}.union_rmv; SYSTEM WAIT VIEW ${DST}.union_rmv; SYSTEM REFRESH VIEW ${EXT}.param_rmv; SYSTEM WAIT VIEW ${EXT}.param_rmv"
rows ${DST}.rt ${EXT}.rt ${DST}.union_rmv ${EXT}.prt

# The arguments of remote() name the tables of another server.
${CLICKHOUSE_CLIENT} -q "
    SELECT replaceAll(create_table_query, '${CLICKHOUSE_DATABASE}', '') FROM system.tables
    WHERE database = '${EXT}' AND name = 'remote_v' FORMAT TSVRaw"

[ "$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DST}.unrelated_v" | sed "s/${DST}/db/g")" = "${unrelated_src_before}" ] && echo "unchanged: ${DST}.unrelated_v" | sed "s/${CLICKHOUSE_DATABASE}//"
[ "$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${EXT}.unrelated_v")" = "${unrelated_ext_before}" ] && echo "unchanged: ${EXT}.unrelated_v" | sed "s/${CLICKHOUSE_DATABASE}//"

echo "-- after a reload"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DST} SYNC; ATTACH DATABASE ${DST}; DETACH DATABASE ${EXT} SYNC; ATTACH DATABASE ${EXT}"
check
${CLICKHOUSE_CLIENT} -q "
    SELECT arraySort(arrayMap((d, t) -> concat(replaceOne(d, '${CLICKHOUSE_DATABASE}', ''), '.', t), dependencies_database, dependencies_table))
    FROM system.tables WHERE database = '${DST}' AND name = 'sample'"

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${DST} SYNC;
    DROP DATABASE IF EXISTS ${EXT} SYNC;
    DROP DATABASE IF EXISTS ${MEM} SYNC;
    DROP DATABASE IF EXISTS ${FSDB} SYNC;
"
