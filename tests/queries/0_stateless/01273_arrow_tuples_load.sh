#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS arrow_tuples"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE arrow_tuples (t1 Tuple(UInt32, UInt32), t2 Tuple(String, String), t3 Tuple(Tuple(UInt32, String), UInt32), t4 Tuple(Tuple(UInt32, UInt32), Tuple(String, String)), t5 Tuple(Array(UInt32), UInt32), t6 Tuple(Tuple(Array(UInt32), Array(UInt32)), Tuple(Array(Array(UInt32)), UInt32)), t7 Array(Tuple(Array(Array(UInt32)), Tuple(Array(Tuple(UInt32, UInt32)), UInt32)))) ENGINE=Memory()"

${CLICKHOUSE_CLIENT} --query="INSERT INTO arrow_tuples VALUES ((1, 2), ('1', '2'), ((1, '1'), 1), ((1, 2), ('1', '2')), ([1,2,3], 1), (([1,2,3], [1,2,3]), ([[1,2,3], [1,2,3]], 1)), [([[1,2,3], [1,2,3]], ([(1, 2), (1, 2)], 1))])"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_tuples FORMAT Arrow" > "${CLICKHOUSE_TMP}"/tuples.arrow

cat "${CLICKHOUSE_TMP}"/tuples.arrow | ${CLICKHOUSE_CLIENT} -q "INSERT INTO arrow_tuples FORMAT Arrow"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM arrow_tuples"
${CLICKHOUSE_CLIENT} --query="DROP TABLE arrow_tuples"
