#!/usr/bin/env bash

#--------------------------------------------
# Description of test result:
#   Test the correctness of the optimization
#   by asserting read marks in the log.
# Relation of read marks and optimization:
#   read marks =
#       the number of monotonic marks filtered through predicates
#       + no monotonic marks count
#--------------------------------------------

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS enum_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS date_test_table;"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE string_test_table (val String) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE fixed_string_test_table (val FixedString(1)) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE signed_integer_test_table (val Int32) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE unsigned_integer_test_table (val UInt32) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE enum_test_table (val Enum16('hello' = 1, 'world' = 2, 'yandex' = 256, 'clickhouse' = 257)) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE date_test_table (val Date) ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;"

${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES string_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES enum_test_table;"
${CLICKHOUSE_CLIENT} --query="SYSTEM STOP MERGES date_test_table;"

${CLICKHOUSE_CLIENT} --query="INSERT INTO string_test_table VALUES ('0'), ('2'), ('2');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO fixed_string_test_table VALUES ('0'), ('2'), ('2');"
# 131072 -> 17 bit is 1
${CLICKHOUSE_CLIENT} --query="INSERT INTO signed_integer_test_table VALUES (-2), (0), (2), (2), (131072), (131073), (131073);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO unsigned_integer_test_table VALUES (0), (2), (2), (131072), (131073), (131073);"
${CLICKHOUSE_CLIENT} --query="INSERT INTO enum_test_table VALUES ('hello'), ('world'), ('world'), ('yandex'), ('clickhouse'), ('clickhouse');"
${CLICKHOUSE_CLIENT} --query="INSERT INTO date_test_table VALUES (1), (2), (2), (256), (257), (257);"

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM string_test_table WHERE toUInt64(val) == 0;" 2>&1 |grep -q "3 marks to read from 1 ranges" && echo "no monotonic int case: String -> UInt64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM fixed_string_test_table WHERE toUInt64(val) == 0;" 2>&1 |grep -q "3 marks to read from 1 ranges" && echo "no monotonic int case: FixedString -> UInt64"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toInt64(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: Int32 -> Int64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toUInt64(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: Int32 -> UInt64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toInt32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: Int32 -> Int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toUInt32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: Int32 -> UInt32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toInt16(val) == 0;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Int32 -> Int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM signed_integer_test_table WHERE toUInt16(val) == 0;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Int32 -> UInt16"

${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toInt64(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: UInt32 -> Int64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toUInt64(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: UInt32 -> UInt64"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toInt32(val) == 0;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: UInt32 -> Int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toUInt32(val) == 0;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: UInt32 -> UInt32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toInt16(val) == 0;" 2>&1 |grep -q "4 marks to read from" && echo "monotonic int case: UInt32 -> Int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM unsigned_integer_test_table WHERE toUInt16(val) == 0;" 2>&1 |grep -q "4 marks to read from" && echo "monotonic int case: UInt32 -> UInt16"


${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toInt32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> Int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toUInt32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> UInt32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toInt16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> Int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toUInt16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Enum16 -> UInt16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toInt8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Enum16 -> Int8"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM enum_test_table WHERE toUInt8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Enum16 -> UInt8"


${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toInt32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Date -> Int32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toUInt32(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Date -> UInt32"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toInt16(val) == 1;" 2>&1 |grep -q "2 marks to read from" && echo "monotonic int case: Date -> Int16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toUInt16(val) == 1;" 2>&1 |grep -q "1 marks to read from" && echo "monotonic int case: Date -> UInt16"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toInt8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Date -> Int8"
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM date_test_table WHERE toUInt8(val) == 1;" 2>&1 |grep -q "5 marks to read from" && echo "monotonic int case: Date -> UInt8"

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=debug/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/g')

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS fixed_string_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS signed_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS unsigned_integer_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS enum_test_table;"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS date_test_table;"
