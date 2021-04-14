#!/usr/bin/env bash

# NOTE: this is a partial copy of the 01683_dist_INSERT_block_structure_mismatch,
# but this test also checks the log messages

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="none" -q "SELECT *  FROM s3('https://s3.mds.yandex.net/clickhouse-test-reports/*/*/functional_stateless_tests_(ubsan)/test_results.tsv',  '$S3_ACCESS_KEY_ID', '$S3_SECRET_ACCESS', 'LineAsString', 'line String') limit 100 FORMAT Null;"
${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="none" -q "SELECT *  FROM s3Cluster('test_cluster_two_shards', 'https://s3.mds.yandex.net/clickhouse-test-reports/*/*/functional_stateless_tests_(ubsan)/test_results.tsv',  '$S3_ACCESS_KEY_ID', '$S3_SECRET_ACCESS', 'LineAsString', 'line String') limit 100 FORMAT Null;"
