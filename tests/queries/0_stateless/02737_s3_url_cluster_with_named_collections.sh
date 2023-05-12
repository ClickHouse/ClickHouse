#!/usr/bin/env bash
# Tags: no-fasttest, no-s3-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "create named collection $CLICKHOUSE_TEST_UNIQUE_NAME as url='http://localhost:11111/test/a.tsv'"
$CLICKHOUSE_CLIENT -q "select * from s3Cluster(test_cluster_one_shard_three_replicas_localhost, $CLICKHOUSE_TEST_UNIQUE_NAME)"
$CLICKHOUSE_CLIENT -q "select * from s3Cluster(test_cluster_one_shard_three_replicas_localhost, $CLICKHOUSE_TEST_UNIQUE_NAME, structure='auto')"
$CLICKHOUSE_CLIENT -q "select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, $CLICKHOUSE_TEST_UNIQUE_NAME)"
$CLICKHOUSE_CLIENT -q "select * from urlCluster(test_cluster_one_shard_three_replicas_localhost, $CLICKHOUSE_TEST_UNIQUE_NAME, structure='auto')"
$CLICKHOUSE_CLIENT -q "drop named collection $CLICKHOUSE_TEST_UNIQUE_NAME"

