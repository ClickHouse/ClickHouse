#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_01527="${CLICKHOUSE_TMP}/01527_clickhouse_local_optimize"
rm -rf "${WORKING_FOLDER_01527}"
mkdir -p "${WORKING_FOLDER_01527}"

# OPTIMIZE was crashing due to lack of temporary volume in local
${CLICKHOUSE_LOCAL} --query "drop database if exists d; create database d; create table d.t engine MergeTree order by a as select 1 a; optimize table d.t final" -- --path="${WORKING_FOLDER_01527}"

rm -rf "${WORKING_FOLDER_01527}"
