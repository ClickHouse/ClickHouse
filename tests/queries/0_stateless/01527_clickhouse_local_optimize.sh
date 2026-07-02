#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_01527="${CLICKHOUSE_TMP}/01527_clickhouse_local_optimize"
rm -rf "${WORKING_FOLDER_01527}"
mkdir -p "${WORKING_FOLDER_01527}"

# OPTIMIZE was crashing due to lack of temporary volume in local
${CLICKHOUSE_LOCAL} --query "drop database if exists ${CLICKHOUSE_DATABASE_1}; create database ${CLICKHOUSE_DATABASE_1}; create table ${CLICKHOUSE_DATABASE_1}.t engine MergeTree order by a as select 1 a; optimize table ${CLICKHOUSE_DATABASE_1}.t final" --path="${WORKING_FOLDER_01527}"

rm -rf "${WORKING_FOLDER_01527}"
