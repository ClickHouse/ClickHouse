#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

mkdir -p "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/

for i in {1..10}
do
   echo \"file"$i"\","$i" > "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file"$i".csv
done

$CLICKHOUSE_CLIENT --query "SELECT * FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file{1..10}.csv', 'CSV', 's String, i UInt32') ORDER BY (i, s)"

rm "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file*.csv
