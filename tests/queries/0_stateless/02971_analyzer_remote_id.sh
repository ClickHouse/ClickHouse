#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_02971"
${CLICKHOUSE_CLIENT} --query="CREATE DATABASE test_02971"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_02971.x ENGINE = MergeTree() ORDER BY number AS SELECT * FROM numbers(2)"
${CLICKHOUSE_LOCAL} --query="SELECT count() FROM remote('127.0.0.{2,3}', 'test_02971.x') SETTINGS allow_experimental_analyzer = 1" 2>&1 \
        | grep -av "ASan doesn't fully support makecontext/swapcontext functions"

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_02971"
