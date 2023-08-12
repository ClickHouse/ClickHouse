#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$(clickhouse-client --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
FILE_NAME=test_02517.data
DATA_FILE=${USER_FILES_PATH:?}/$FILE_NAME

touch $DATA_FILE

echo "TSV"

echo -e "42\tSome string\t[1, 2, 3, 4]\t(1, 2, 3)
42\tabcd\t[]\t(4, 5, 6)" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"

echo "CSV"

echo -e "a,b,c,\"\nd\"
a,b,c,\"\nd\"" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"

echo "CSV with dates"

echo -e "1,2021-04-01 00:00:18,2021-04-01 00:21:54,1,8.40,1,N,79,116,1,25.5,3,0.5,5.85,0,0.3,35.15,2.5
1,2021-04-01 00:42:37,2021-04-01 00:46:23,1,.90,1,N,75,236,2,5,3,0.5,0,0,0.3,8.8,2.5" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"

echo "ClickHouse logs"

echo -e "2022.11.17 11:07:30.825405 [ 395843 ] {345bf223-db80-4772-9a07-73542321e715::202211_15349_15604_53} <Debug> MergeTask::PrepareStage: Merging 6 parts: from 202211_15349_15599_52 to 202211_15604_15604_0 into Compact
2022.11.17 11:07:30.826372 [ 395843 ] {345bf223-db80-4772-9a07-73542321e715::202211_15349_15604_53} <Debug> MergeTask::PrepareStage: Selected MergeAlgorithm: Horizontal" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"

echo "Syslog"

echo -e "Nov 13 10:29:56 PC chronyd[227]: Selected source PHC0
Nov 13 13:24:02 PC kernel: [91566.562562] hv_utils: TimeSync IC version 4.0" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"

echo "JSONL"

echo -e "{\"msg\":1,\"type\":\"log\",\"nested\":{\"object\":\"a\",\"field\":\"something_a\"}}
{\"nested\":{\"object\":\"b\",\"field\":\"\"},\"msg\":2,\"type\":\"log\",\"skipped\":\"this will be skpped\"}" > $DATA_FILE

$CLICKHOUSE_CLIENT -q "desc file('$FILE_NAME', 'Freeform')"
$CLICKHOUSE_CLIENT -q "select * from file('$FILE_NAME', 'Freeform')"
