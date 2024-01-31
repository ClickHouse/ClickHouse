#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_files_path=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
mkdir -p ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/
rm -rf ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
cp $CUR_DIR/data_json/btc_transactions.json ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS btc"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE btc (data JSON) ENGINE = MergeTree ORDER BY tuple()" --allow_experimental_object_type 1

${CLICKHOUSE_CLIENT} -q "INSERT INTO btc SELECT * FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/btc_transactions.json', 'JSONAsObject')"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM btc WHERE NOT ignore(*)"
${CLICKHOUSE_CLIENT} -q "DESC btc SETTINGS describe_extend_object_types = 1"

${CLICKHOUSE_CLIENT} -q "SELECT avg(data.fee), median(data.fee) FROM btc"

${CLICKHOUSE_CLIENT} -q "SELECT avg(length(data.inputs.prev_out.spending_outpoints) AS outpoints_length), median(outpoints_length) FROM btc"

${CLICKHOUSE_CLIENT} -q "SELECT data.out.spending_outpoints AS outpoints FROM btc WHERE arrayExists(x -> notEmpty(x), outpoints)"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS btc"

rm ${user_files_path}/${CLICKHOUSE_TEST_UNIQUE_NAME}/btc_transactions.json
