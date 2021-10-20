#!/usr/bin/env bash
# Tags: no-unbundled, no-fasttest

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test_empty_data"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_empty_data (x Int8) ENGINE = Memory"

(echo "INSERT INTO test_empty_data FORMAT Arrow" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x FORMAT Arrow") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"
(echo "INSERT INTO test_empty_data FORMAT Arrow" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x LIMIT 0 FORMAT Arrow") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"
(echo "INSERT INTO test_empty_data FORMAT ArrowStream" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x FORMAT ArrowStream") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"
(echo "INSERT INTO test_empty_data FORMAT ArrowStream" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x LIMIT 0 FORMAT ArrowStream") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"
(echo "INSERT INTO test_empty_data FORMAT Parquet" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x FORMAT Parquet") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"
(echo "INSERT INTO test_empty_data FORMAT Parquet" && ${CLICKHOUSE_CLIENT} --query="SELECT 1 AS x LIMIT 0 FORMAT Parquet") | ${CLICKHOUSE_CLIENT}
${CLICKHOUSE_CLIENT} --query="SELECT count() FROM test_empty_data"

${CLICKHOUSE_CLIENT} -q "DROP TABLE test_empty_data"
