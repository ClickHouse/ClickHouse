#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.contributors"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.contributors AS system.contributors"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.numbers"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.numbers AS system.numbers"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.contributors ORDER BY NAME FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.contributors FORMAT Parquet"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM system.numbers LIMIT 100000 FORMAT Parquet" | ${CLICKHOUSE_CLIENT} --query="INSERT INTO test.numbers FORMAT Parquet"

${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.contributors"
${CLICKHOUSE_CLIENT} --query="SELECT * FROM test.numbers LIMIT 10"

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.contributors"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.numbers"
