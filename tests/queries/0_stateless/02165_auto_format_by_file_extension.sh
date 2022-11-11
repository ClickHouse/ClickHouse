#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/hello.csv ] && rm "${CLICKHOUSE_TMP}"/hello.csv
[ -e "${CLICKHOUSE_TMP}"/world.csv.gz ] && rm "${CLICKHOUSE_TMP}"/world.csv.gz
[ -e "${CLICKHOUSE_TMP}"/hello.world.csv ] && rm "${CLICKHOUSE_TMP}"/hello.world.csv
[ -e "${CLICKHOUSE_TMP}"/hello.world.csv.xz ] && rm "${CLICKHOUSE_TMP}"/hello.world.csv.xz
[ -e "${CLICKHOUSE_TMP}"/.htaccess.json ] && rm "${CLICKHOUSE_TMP}"/.htaccess.json
[ -e "${CLICKHOUSE_TMP}"/example.com. ] && rm "${CLICKHOUSE_TMP}"/example.com.
[ -e "${CLICKHOUSE_TMP}"/museum...protobuf ] && rm "${CLICKHOUSE_TMP}"/museum...protobuf

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS out_tb_02165;"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE out_tb_02165 (id UInt64, name String) Engine=Memory;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO out_tb_02165 Values(1, 'one'), (2, 'tow');"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE in_tb_02165 (id UInt64, name String) Engine=Memory;"


${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/hello.csv';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/hello.csv' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/world.csv.gz';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/world.csv.gz' COMPRESSION 'gz' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/hello.world.csv';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/hello.world.csv' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/hello.world.csv.xz';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/hello.world.csv.xz' COMPRESSION 'xz' FORMAT CSV;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/example.com.';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/example.com.' FORMAT TabSeparated;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/museum...JSONEachRow';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/museum...JSONEachRow';"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE in_tb_02165 FROM INFILE '${CLICKHOUSE_TMP}/world.csv.gz';"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM in_tb_02165;"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE in_tb_02165;"


${CLICKHOUSE_CLIENT} --query "SELECT * FROM out_tb_02165 INTO OUTFILE '${CLICKHOUSE_TMP}/.htaccess.json';"
head -n 26 ${CLICKHOUSE_TMP}/.htaccess.json

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS out_tb_02165;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS in_tb_02165;"

rm "${CLICKHOUSE_TMP}"/hello.csv
rm "${CLICKHOUSE_TMP}"/world.csv.gz
rm "${CLICKHOUSE_TMP}"/hello.world.csv
rm "${CLICKHOUSE_TMP}"/hello.world.csv.xz
rm "${CLICKHOUSE_TMP}"/.htaccess.json
rm "${CLICKHOUSE_TMP}"/example.com.
rm "${CLICKHOUSE_TMP}"/museum...JSONEachRow
