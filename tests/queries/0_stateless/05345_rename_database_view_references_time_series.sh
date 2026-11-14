#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Tag no-replicated-database: the test creates its own databases, the Replicated ones are in 05343.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE rewrites the target tables in the renamed database stored in the TimeSeries tables of every database.
# Adapted from #110982.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
EXT="${CLICKHOUSE_DATABASE}_ext"

${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${SRC} ENGINE = Atomic;
    CREATE DATABASE ${EXT} ENGINE = Atomic;
"
for suffix in "" 2; do
    ${CLICKHOUSE_CLIENT} -q "
        CREATE TABLE ${SRC}.samples${suffix} (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
        CREATE TABLE ${SRC}.tags${suffix} (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String),
            min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY id;
        CREATE TABLE ${SRC}.families${suffix} (metric_family String, type String, unit String, help String)
            ENGINE = ReplacingMergeTree ORDER BY metric_family;
    "
done
# Only a table of version 9 or earlier can have external target tables.
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${SRC}.ts ENGINE = TimeSeries SETTINGS version = 7 SAMPLES ${SRC}.samples TAGS ${SRC}.tags METRIC FAMILIES ${SRC}.families;
    CREATE TABLE ${EXT}.ts ENGINE = TimeSeries SETTINGS version = 7 SAMPLES ${SRC}.samples2 TAGS ${SRC}.tags2 METRIC FAMILIES ${SRC}.families2;
"

${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${SRC} TO ${DST}"

function check()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
        INSERT INTO ${DST}.ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.0)]);
        INSERT INTO ${EXT}.ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.0)]);
    "
    ${CLICKHOUSE_CLIENT} -q "
        SELECT '${DST}.samples: ' || toString(count()) FROM ${DST}.samples;
        SELECT '${DST}.samples2: ' || toString(count()) FROM ${DST}.samples2;
    " | sed "s/${CLICKHOUSE_DATABASE}//"
}

echo "-- after the rename"
check
echo "occurrences of the old name: $(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE ${DST}.ts; SHOW CREATE TABLE ${EXT}.ts" | grep -o -F "${SRC}" | wc -l)"

echo "-- after a reload"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DST} SYNC; ATTACH DATABASE ${DST}; DETACH DATABASE ${EXT} SYNC; ATTACH DATABASE ${EXT}"
check

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${EXT} SYNC;
    DROP DATABASE IF EXISTS ${DST} SYNC;
"
