#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (f1 String, f2 String) ENGINE = Memory"

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary 'insert into test (f1, f2) format TSV 1' 2>&1 | grep -F '< HTTP/'

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
