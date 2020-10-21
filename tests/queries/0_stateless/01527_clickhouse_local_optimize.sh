#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER_01527="${CLICKHOUSE_TMP}/01527_clickhouse_local_optimize"

rm -rf "${WORKING_FOLDER_01527}"
mkdir -p "${WORKING_FOLDER_01527}/metadata/local/"

# OPTIMIZE was crashing due to lack of temporary volume in local
${CLICKHOUSE_LOCAL} --query "drop database if exists d; create database d; create table d.t engine MergeTree order by a as select 1 a; optimize table d.t final" -- --path="${WORKING_FOLDER_01527}"

# Some extra (unrealted) scenarios of clickhouse-local usage.

## 1. Imagine we want to process this file:
cat <<EOF > "${WORKING_FOLDER_01527}/data.csv"
1,2020-01-01,"String"
2,2020-02-02,"Another string"
3,2020-03-03,"One more string"
4,2020-01-02,"String for first partition"
EOF

## 2. that is the metadata for the table we want to fill
## schema should match the schema of the table from server
## (the easiest way is just to copy it from the server)
cat <<EOF > "${WORKING_FOLDER_01527}/metadata/local/test.sql"
ATTACH TABLE local.test (id UInt64, d Date, s String) Engine=MergeTree ORDER BY id PARTITION BY toYYYYMM(d);
EOF

## 3a. that is the metadata for the input file we want to read
## it should match the structure of source file
## use stdin to read from pipe
cat <<EOF > "${WORKING_FOLDER_01527}/metadata/local/stdin.sql"
ATTACH TABLE local.stdin (id UInt64, d Date, s String) Engine=File(CSV, stdin);
EOF

## 3b. Instead of stdin you can use file path
cat <<EOF > "${WORKING_FOLDER_01527}/metadata/local/data_csv.sql"
ATTACH TABLE local.data_csv (id UInt64, d Date, s String) Engine=File(CSV, '${WORKING_FOLDER_01527}/data.csv');
EOF

## All preparations done, the rest is simple:

# option a (if 3a used) with pipe / reading stdin (truncate was added for the test)
cat "${WORKING_FOLDER_01527}/data.csv" | ${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.stdin; SELECT * FROM local.test ORDER BY id; TRUNCATE TABLE local.test;" -- --path="${WORKING_FOLDER_01527}"

# option b (if 3b used) 0 with filepath  (truncate was added for the test)
${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.data_csv; SELECT * FROM local.test ORDER BY id; TRUNCATE TABLE local.test;" -- --path="${WORKING_FOLDER_01527}"

rm -rf "${WORKING_FOLDER_01527}"