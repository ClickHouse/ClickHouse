#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PATH=${USER_FILES_PATH}/${CURR_DATABASE}_db1

sqlite3 "${DB_PATH}" 'DROP TABLE IF EXISTS t1'
sqlite3 "${DB_PATH}" 'CREATE TABLE t1(c0 INT,c1 INT);'

chmod ugo+w "${DB_PATH}"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t0 (c0 Int) ENGINE = Memory();"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t1 (c0 Int, c1 Int) ENGINE = SQLite('${DB_PATH}', 't1');";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE ${CLICKHOUSE_DATABASE}.t2 (c0 Int, c1 Int) ENGINE = Memory();";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t0 (c0) VALUES (1);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t1 (c0, c1) VALUES (-3, 0), (1, 0), (-2, 0);";
${CLICKHOUSE_CLIENT} --query="INSERT INTO TABLE ${CLICKHOUSE_DATABASE}.t2 (c0, c1) VALUES (-3, 0), (1, 0), (-2, 0);";

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t0 tx RIGHT JOIN ${CLICKHOUSE_DATABASE}.t1 ty ON ty.c1 = tx.c0 WHERE tx.c0 < 1;"
echo "-----"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM ${CLICKHOUSE_DATABASE}.t0 tx RIGHT JOIN ${CLICKHOUSE_DATABASE}.t2 ty ON ty.c1 = tx.c0 WHERE tx.c0 < 1;"
