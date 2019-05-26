#!/usr/bin/env bash

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (val Int64) engine = Memory;"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT 1;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT NULL AS src;" 2>&1 | grep -oF 'while converting source column src to destination column val';
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number % 2 ? 1 : NULL AS src FROM numbers(10);" 2>&1 | grep -oF 'while converting source column src to destination column val';

${CLICKHOUSE_CLIENT} --query "DROP TABLE test;"
