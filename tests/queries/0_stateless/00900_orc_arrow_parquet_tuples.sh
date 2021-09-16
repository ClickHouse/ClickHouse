#!/usr/bin/env bash
# Tags: no-unbundled, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS tuples";
${CLICKHOUSE_CLIENT} --query="CREATE TABLE tuples (t1 Tuple(UInt32, UInt32), t2 Tuple(String, String), t3 Tuple(Tuple(UInt32, String), UInt32), t4 Tuple(Tuple(UInt32, UInt32), Tuple(String, String)), t5 Tuple(Array(UInt32), UInt32), t6 Tuple(Tuple(Array(UInt32), Array(UInt32)), Tuple(Array(Array(UInt32)), UInt32)), t7 Array(Tuple(Array(Array(UInt32)), Tuple(Array(Tuple(UInt32, UInt32)), UInt32)))) ENGINE=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO tuples VALUES ((1, 2), ('1', '2'), ((1, '1'), 1), ((1, 2), ('1', '2')), ([1,2,3], 1), (([1,2,3], [1,2,3]), ([[1,2,3], [1,2,3]], 1)), [([[1,2,3], [1,2,3]], ([(1, 2), (1, 2)], 1))])"

formats="Arrow Parquet ORC";

for format in ${formats}; do
    echo $format

    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM tuples FORMAT $format" > "${CLICKHOUSE_TMP}"/tuples
    ${CLICKHOUSE_CLIENT} --query="TRUNCATE TABLE tuples"
    cat "${CLICKHOUSE_TMP}"/tuples | ${CLICKHOUSE_CLIENT} -q "INSERT INTO tuples FORMAT $format"
    ${CLICKHOUSE_CLIENT} --query="SELECT * FROM tuples"
done

${CLICKHOUSE_CLIENT} --query="DROP TABLE tuples"
