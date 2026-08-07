#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

#____________________CLIENT__________________
# clear files from previous tests.
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output.gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output.gz
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz.gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz.gz
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz_to_decomp ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz_to_decomp
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_to_decomp ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_to_decomp

# create files using compression method and without it to check that both queries work correct
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' FORMAT TabSeparated;"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz' COMPRESSION 'GZ' FORMAT TabSeparated;"

# check content of files
cp ${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp.gz
gunzip ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp.gz
cat ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp

cp ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp.gz
gunzip ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp.gz
cat ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp

# create table to check inserts
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_compression_keyword;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_compression_keyword (text String) Engine=Memory;"

# insert them
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' FORMAT TabSeparated;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' COMPRESSION 'gz' FORMAT TabSeparated;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz' COMPRESSION 'gz' FORMAT TabSeparated;"

# check result
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_compression_keyword;"

# delete all created elements
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_compression_keyword;"

#____________________LOCAL__________________
# clear files from previous tests.
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output.gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output.gz
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz
[ -e "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz.gz ] && rm "${CLICKHOUSE_TMP}"/test_comp_for_input_and_output_without_gz.gz

# create files using compression method and without it to check that both queries work correct
${CLICKHOUSE_LOCAL} --query "SELECT * FROM (SELECT 'Hello, World! From local.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' FORMAT TabSeparated;"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM (SELECT 'Hello, World! From local.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz' COMPRESSION 'GZ' FORMAT TabSeparated;"

# check content of files
cp ${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp.gz
gunzip ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp.gz
cat ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp

cp ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp.gz
gunzip ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp.gz
cat ${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp

# create table to check inserts
${CLICKHOUSE_LOCAL} --query "
DROP TABLE IF EXISTS test_compression_keyword;
CREATE TABLE test_compression_keyword (text String) Engine=Memory;
INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' FORMAT TabSeparated;
INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz' COMPRESSION 'gz' FORMAT TabSeparated;
INSERT INTO TABLE test_compression_keyword FROM INFILE '${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz' COMPRESSION 'gz' FORMAT TabSeparated;
SELECT * FROM test_compression_keyword;
"

# delete all created elements
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_to_decomp"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output.gz"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz_to_decomp"
rm -f "${CLICKHOUSE_TMP}/test_comp_for_input_and_output_without_gz"
