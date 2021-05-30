#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_00690;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_00690 (val Int64) engine = Memory;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT 1;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT NULL AS src;" 2>&1 | grep -oF 'while converting source column src to destination column val';
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_00690 SELECT number % 2 ? 1 : NULL AS src FROM numbers(10);" 2>&1 | grep -oF 'while converting source column src to destination column val';

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_00690;"
