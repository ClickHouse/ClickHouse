#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/

for i in {1..10}
do
   echo \"file"$i"\","$i" > "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file"$i".csv
done

$CLICKHOUSE_CLIENT --query "SELECT * FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file{1..10}.csv', 'CSV', 's String, i UInt32') ORDER BY (i, s)"

rm "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file*.csv
