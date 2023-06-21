#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IN_PATH="${CLICKHOUSE_TMP}/test_infile"
OUT_PATH="${CLICKHOUSE_TMP}/test_outfile"

echo "Hello" > "${IN_PATH}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_infile;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_infile (word String) ENGINE=Memory();"

# 1.

${CLICKHOUSE_CLIENT} --param_in "${IN_PATH}" -q "INSERT INTO test_infile FROM INFILE {in:String} FORMAT CSV;"
${CLICKHOUSE_CLIENT} --param_out "${OUT_PATH}" -q "SELECT * FROM test_infile INTO OUTFILE {out:String} FORMAT CSV;"

cat "${OUT_PATH}"
rm "${OUT_PATH}"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_infile;"

# 2.

${CLICKHOUSE_CLIENT} --multiquery -q "
    SET param_in = '${IN_PATH}';
    SET param_out = '${OUT_PATH}';
    INSERT INTO test_infile FROM INFILE {in:String} FORMAT CSV;
    SELECT * FROM test_infile INTO OUTFILE {out:String} FORMAT CSV;"

cat "${OUT_PATH}"

rm "${IN_PATH}"
rm "${OUT_PATH}"

# 3.

${CLICKHOUSE_CLIENT} --param_in "${IN_PATH}" -q "INSERT INTO test_infile FROM INFILE {in:Identifier} FORMAT CSV;" 2>&1 | grep -c "UNKNOWN_TYPE"
${CLICKHOUSE_CLIENT} --param_out "${OUT_PATH}" -q "SELECT * FROM test_infile INTO OUTFILE {out:Identifier} FORMAT CSV;" 2>&1 | grep -c "UNKNOWN_TYPE"

${CLICKHOUSE_CLIENT} --param_in "${IN_PATH}" -q "INSERT INTO test_infile FROM INFILE {in:Int32} FORMAT CSV;" 2>&1 | grep -c "BAD_QUERY_PARAMETER"
${CLICKHOUSE_CLIENT} --param_out "${OUT_PATH}" -q "SELECT * FROM test_infile INTO OUTFILE {out:Int32} FORMAT CSV;" 2>&1 | grep -c "BAD_QUERY_PARAMETER"

${CLICKHOUSE_CLIENT} -q "INSERT INTO test_infile FROM INFILE {in:String} FORMAT CSV;" 2>&1 | grep -c "UNKNOWN_QUERY_PARAMETER"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM test_infile INTO OUTFILE {out:String} FORMAT CSV;" 2>&1 | grep -c "UNKNOWN_QUERY_PARAMETER"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_infile;"