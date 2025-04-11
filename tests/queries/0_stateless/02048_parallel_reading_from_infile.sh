#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel.gz ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel.gz
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_1
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_2
[ -e "${CLICKHOUSE_TMP}"/test_infile_parallel ] && rm "${CLICKHOUSE_TMP}"/test_infile_parallel_3

echo -e "102\t2" > "${CLICKHOUSE_TMP}"/test_infile_parallel
echo -e "102\tsecond" > "${CLICKHOUSE_TMP}"/test_infile_parallel_1
echo -e "103\tfirst" > "${CLICKHOUSE_TMP}"/test_infile_parallel_2
echo -e "103" > "${CLICKHOUSE_TMP}"/test_infile_parallel_3

gzip "${CLICKHOUSE_TMP}"/test_infile_parallel

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS test_infile_parallel;
CREATE TABLE test_infile_parallel (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
SET input_format_allow_errors_num=1;
INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;
SELECT count() FROM test_infile_parallel WHERE Value='first';
SELECT count() FROM test_infile_parallel WHERE Value='second';
EOF

# Error code is 27 (DB::ParsingException). It is not ignored.
${CLICKHOUSE_CLIENT}  -m --query "DROP TABLE IF EXISTS test_infile_parallel;
CREATE TABLE test_infile_parallel (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
SET input_format_allow_errors_num=0;
INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;
" 2>&1 | grep -q "27" && echo "Correct" || echo 'Fail'

${CLICKHOUSE_LOCAL} <<EOF
DROP TABLE IF EXISTS test_infile_parallel; 
SET input_format_allow_errors_num=1;
CREATE TABLE test_infile_parallel (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory(); 
INSERT INTO test_infile_parallel FROM INFILE '${CLICKHOUSE_TMP}/test_infile_parallel*' FORMAT TSV;
SELECT count() FROM test_infile_parallel WHERE Value='first';
SELECT count() FROM test_infile_parallel WHERE Value='second';
EOF
