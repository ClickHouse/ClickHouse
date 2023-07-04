#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_from_string"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2 (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2_string (data String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ghdata_2_from_string (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO ghdata_2 FORMAT JSONAsObject"
cat $CUR_DIR/data_json/ghdata_sample.json | ${CLICKHOUSE_CLIENT} -q "INSERT INTO ghdata_2_string FORMAT JSONAsString"

${CLICKHOUSE_CLIENT} -q "INSERT INTO ghdata_2_from_string SELECT data FROM ghdata_2_string"

${CLICKHOUSE_CLIENT} -q "SELECT \
    (SELECT toTypeName(any(data)), sum(cityHash64(flattenTuple(data))) FROM ghdata_2_from_string) = \
    (SELECT toTypeName(any(data)), sum(cityHash64(flattenTuple(data))) FROM ghdata_2)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_string"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ghdata_2_from_string"
