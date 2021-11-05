#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel.gz ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel.gz
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_1
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_2


echo -e "102\t2" > "${CLICKHOUSE_TMP}"/test_infile_parallel

echo -e "102\tsecond" > "${CLICKHOUSE_TMP}"/test_infile_parallel_1

echo -e "103\tfirst" > "${CLICKHOUSE_TMP}"/test_infile_parallel_2

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_infile_parallel;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_infile_parallel (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();"
${CLICKHOUSE_CLIENT} --query "SET input_format_tsv_enum_as_number = 0;"

gzip "${CLICKHOUSE_TMP}"/test_infile_parallel

# Check that settings are applied
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;"  
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_infile_parallel WHERE Value='first';"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_infile_parallel WHERE Value='second';"

${CLICKHOUSE_CLIENT} --query "SET input_format_tsv_enum_as_number = 1;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_infile_parallel WHERE Value='first';"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM test_infile_parallel WHERE Value='second';"

${CLICKHOUSE_LOCAL} --multiquery <<EOF
DROP TABLE IF EXISTS test_infile_parallel; 
CREATE TABLE test_infile_parallel (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory(); 
INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;
SELECT count() FROM test_infile_parallel WHERE Value='first';
SELECT count() FROM test_infile_parallel WHERE Value='second';
EOF
