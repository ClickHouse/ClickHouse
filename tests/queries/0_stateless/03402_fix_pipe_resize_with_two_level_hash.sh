#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/

for i in {1..10}; do
    file_name="${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file"$i".csv

    for ((j = 1; j <= 10000; j++)); do
        random_num=$((RANDOM % 1000000))
        echo "$random_num" >> "$file_name"
    done
done

$CLICKHOUSE_CLIENT --query "EXPLAIN PIPELINE SELECT cityHash64(n) % 65536 AS n, sum(1) FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file{1..10}.csv', 'CSV', 'n UInt32') GROUP BY n SETTINGS distributed_aggregation_memory_efficient=1, max_threads = 8"

$CLICKHOUSE_CLIENT --query "SELECT cityHash64(n) % 65536 AS n, sum(1) FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/file{1..10}.csv', 'CSV', 'n UInt32') GROUP BY n FORMAT NULL SETTINGS distributed_aggregation_memory_efficient=1, max_threads = 8"

rm "${USER_FILES_PATH}"/"${CLICKHOUSE_TEST_UNIQUE_NAME}"/file*.csv
