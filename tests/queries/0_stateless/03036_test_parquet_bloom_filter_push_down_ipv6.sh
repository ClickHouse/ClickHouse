#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/ipv6_bloom_filter.gz.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/ipv6_bloom_filter.gz.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

echo "bloom filter is off, row groups should be read"
echo "expect rows_read = select count()"
${CLICKHOUSE_CLIENT} --query="select ipv6 from file('${DATA_FILE_USER_PATH}', Parquet, 'ipv6 IPv6') where ipv6 = '7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995' Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on for ipv6, row groups should also be read since there is only one. Below queries just make sure the data is properly returned"
${CLICKHOUSE_CLIENT} --query="select ipv6 from file('${DATA_FILE_USER_PATH}', Parquet, 'ipv6 IPv6') where ipv6 = '7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995' Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
${CLICKHOUSE_CLIENT} --query="select ipv6 from file('${DATA_FILE_USER_PATH}', Parquet, 'ipv6 IPv6') where ipv6 = toIPv6('7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995') Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
${CLICKHOUSE_CLIENT} --query="select toIPv6(ipv6) from file('${DATA_FILE_USER_PATH}', Parquet) where ipv6 = toIPv6('7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995') Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "non existent ipv6, row group should be skipped"
${CLICKHOUSE_CLIENT} --query="select ipv6 from file('${DATA_FILE_USER_PATH}', Parquet, 'ipv6 IPv6') where ipv6 = 'fafe:b9d4:e754:4e78:8783:37f5:b2ea:9995' Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
${CLICKHOUSE_CLIENT} --query="select ipv6 from file('${DATA_FILE_USER_PATH}', Parquet, 'ipv6 IPv6') where ipv6 = toIPv6('fafe:b9d4:e754:4e78:8783:37f5:b2ea:9995') Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
${CLICKHOUSE_CLIENT} --query="select toIPv6(ipv6) from file('${DATA_FILE_USER_PATH}', Parquet) where ipv6 = toIPv6('fafe:b9d4:e754:4e78:8783:37f5:b2ea:9995') Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'
