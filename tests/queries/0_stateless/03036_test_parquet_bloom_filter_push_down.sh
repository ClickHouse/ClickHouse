#!/usr/bin/env bash
# Tags: no-ubsan, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

mkdir -p "${WORKING_DIR}"

DATA_FILE="${CUR_DIR}/data_parquet/multi_column_bf.gz.parquet"

DATA_FILE_USER_PATH="${WORKING_DIR}/multi_column_bf.gz.parquet"

cp ${DATA_FILE} ${DATA_FILE_USER_PATH}

${CLICKHOUSE_CLIENT} --query="select count(*) from file('${DATA_FILE_USER_PATH}', Parquet) SETTINGS use_cache_for_count_from_files=false;"

echo "bloom filter is off, all row groups should be read"
echo "expect rows_read = select count()"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where string='PFJH' or flba='WNMM' order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false"  | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, some row groups should be skipped"
echo "expect rows_read much less than select count()"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where string='PFJH' or flba='WNMM' order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false"  | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, but where predicate contains data from 2 row groups out of 3."
echo "Rows read should be less than select count, but greater than previous selects"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where string='PFJH' or string='ZHZK' order by uint16_logical asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "bloom filter is on, but where predicate contains data from all row groups"
echo "expect rows_read = select count()"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where string='PFJH' or string='ZHZK' or uint64_logical=374 order by uint16_logical asc Format JSON SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;" | jq 'del(.meta,.statistics.elapsed)'

echo "IN check"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where string in ('PFJH', 'ZHZK') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "tuple in case, bf is off."
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where (string, flba) in ('PFJH', 'GKJC') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "tuple in case, bf is on."
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where (string, flba) in ('PFJH', 'GKJC') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is off"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where (string, flba) in (('NON1', 'NON1'), ('PFJH', 'GKJC'), ('NON2', 'NON2')) order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is on"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where (string, flba) in (('NON1', 'NON1'), ('PFJH', 'GKJC'), ('NON2', 'NON2')) order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "complex tuple in case, bf is on. Non existent"
${CLICKHOUSE_CLIENT} --query="select string, flba from file('${DATA_FILE_USER_PATH}', Parquet) where (string, flba) in (('NON1', 'NON1'), ('NON2', 'NON2'), ('NON3', 'NON3')) order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for json column. BF is off"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where json = '{\"key\":38, \"value\":\"NXONM\"}' order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for json column. BF is on"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where json = '{\"key\":38, \"value\":\"NXONM\"}' order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for ipv4 column. BF is off"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where ipv4 = IPv4StringToNum('0.0.1.143') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=false, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for ipv4 column. BF is on"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where ipv4 = IPv4StringToNum('0.0.1.143') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for ipv4 column. BF is on, but toIPv4(ipv4) = toIPv4('0.0.1.143') is not supported. Should read everything"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where toIPv4(ipv4) = toIPv4('0.0.1.143') order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

echo "Bloom filter for ipv4 column. BF is on, but (toIPv4(ipv4)) in (toIPv4('0.0.1.143')) is not supported. Should read everything"
${CLICKHOUSE_CLIENT} --query="select json from file('${DATA_FILE_USER_PATH}', Parquet) where (toIPv4(ipv4)) in (toIPv4('0.0.1.143')) order by uint16_logical asc FORMAT Json SETTINGS input_format_parquet_bloom_filter_push_down=true, input_format_parquet_filter_push_down=false;"  | jq 'del(.meta,.statistics.elapsed)'

rm -rf ${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}/*
