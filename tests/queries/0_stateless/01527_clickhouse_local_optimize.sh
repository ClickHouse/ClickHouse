#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/01527_clickhouse_local_optimize"

rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/metadata/local/"

## 1. Imagine we want to process this file:
cat <<EOF > "${WORKING_FOLDER}/data.csv"
1,2020-01-01,"String"
2,2020-02-02,"Another string"
3,2020-03-03,"One more string"
4,2020-01-02,"String for first partition"
EOF

## 2. that is the metadata for the table we want to fill
## schema should match the schema of the table from server
## (the easiest way is just to copy it from the server)

## I've added sleepEachRow(0.5) here just to mimic slow insert
cat <<EOF > "${WORKING_FOLDER}/metadata/local/test.sql"
ATTACH TABLE local.test (id UInt64, d Date, s String, x MATERIALIZED sleepEachRow(0.5)) Engine=MergeTree ORDER BY id PARTITION BY toYYYYMM(d);
EOF

## 3a. that is the metadata for the input file we want to read
## it should match the structure of source file

## use stdin to read from pipe
cat <<EOF > "${WORKING_FOLDER}/metadata/local/stdin.sql"
ATTACH TABLE local.stdin (id UInt64, d Date, s String) Engine=File(CSV, stdin);
EOF

## 3b. Instead of stdin you can use file path
cat <<EOF > "${WORKING_FOLDER}/metadata/local/data_csv.sql"
ATTACH TABLE local.data_csv (id UInt64, d Date, s String) Engine=File(CSV, '${WORKING_FOLDER}/data.csv');
EOF

## All preparations done,
## the rest is simple:

# option a (if 3a used) with pipe / reading stdin
cat "${WORKING_FOLDER}/data.csv" | ${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.stdin" -- --path="${WORKING_FOLDER}"

# option b (if 3b used) 0 with filepath
${CLICKHOUSE_LOCAL} --query "INSERT INTO local.test SELECT * FROM local.data_csv" -- --path="${WORKING_FOLDER}"

# now you can check what was inserted (i did both options so i have doubled data)
${CLICKHOUSE_LOCAL} --query "SELECT _part,* FROM local.test ORDER BY id, _part" -- --path="${WORKING_FOLDER}"

# But you can't do OPTIMIZE (local will die with coredump) :) That would be too good
clickhouse-local --query "OPTIMIZE TABLE local.test FINAL" -- --path="${WORKING_FOLDER}"

# now you can check what was inserted (i did both options so i have doubled data)
${CLICKHOUSE_LOCAL} --query "SELECT _part,* FROM local.test ORDER BY id, _part" -- --path="${WORKING_FOLDER}"

## now you can upload those parts to a server (in detached subfolder) and attach them.
rm -rf "${WORKING_FOLDER}"