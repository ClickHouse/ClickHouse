#!/usr/bin/env bash
# Tags: no-fasttest

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Data preparation.

echo -e "1,1\n2,a\nb,3\n4,4\n5,c\n6,6" > ${CLICKHOUSE_USER_FILES_UNIQUE}/a.csv

${CLICKHOUSE_CLIENT} --query "drop table if exists data;"
${CLICKHOUSE_CLIENT} --query "create table data (A UInt8, B UInt8) engine=MergeTree() order by A;"

# Server side
${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 4 --input_format_record_errors_file_path "${CLICKHOUSE_TEST_UNIQUE_NAME}/errors_server" --query "insert into data select * from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/a.csv', 'CSV', 'c1 UInt8, c2 UInt8');"
sleep 2
${CLICKHOUSE_CLIENT} --query "select * except (time) from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/errors_server', 'CSV', 'time DateTime, database Nullable(String), table Nullable(String), offset UInt32, reason String, raw_data String');"

# Client side
${CLICKHOUSE_CLIENT} --input_format_allow_errors_num 4 --input_format_record_errors_file_path "${CLICKHOUSE_USER_FILES_UNIQUE}/errors_client" --query "insert into data(A, B) format CSV" < ${CLICKHOUSE_USER_FILES_UNIQUE}/a.csv
sleep 2
${CLICKHOUSE_CLIENT} --query "select * except (time) from file('${CLICKHOUSE_TEST_UNIQUE_NAME}/errors_client', 'CSV', 'time DateTime, database Nullable(String), table Nullable(String), offset UInt32, reason String, raw_data String');"

# Restore
${CLICKHOUSE_CLIENT} --query "drop table if exists data;"
rm ${CLICKHOUSE_USER_FILES_UNIQUE}/a.csv
rm ${CLICKHOUSE_USER_FILES_UNIQUE}/errors_server
rm ${CLICKHOUSE_USER_FILES_UNIQUE}/errors_client
