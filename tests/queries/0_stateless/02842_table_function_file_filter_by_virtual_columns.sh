#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "1" > $CLICKHOUSE_TEST_UNIQUE_NAME.data1.tsv
echo "2" > $CLICKHOUSE_TEST_UNIQUE_NAME.data2.tsv
echo "3" > $CLICKHOUSE_TEST_UNIQUE_NAME.data3.tsv

$CLICKHOUSE_LOCAL --print-profile-events -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv', auto, 'x UInt64') where _file like '%data1%' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"

$CLICKHOUSE_LOCAL --print-profile-events -q "select * from file('$CLICKHOUSE_TEST_UNIQUE_NAME.data{1,2,3}.tsv', auto, 'x UInt64') where _path like '%data1%' format Null" 2>&1 | grep -F -c "EngineFileLikeReadFiles: 1"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.data*

