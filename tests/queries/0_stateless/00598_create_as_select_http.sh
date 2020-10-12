#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test_00598"
$CLICKHOUSE_CURL -sS -d 'CREATE TABLE test_00598 ENGINE = Memory AS SELECT 1' "$CLICKHOUSE_URL"
$CLICKHOUSE_CLIENT --query="SELECT * FROM test_00598"
$CLICKHOUSE_CLIENT --query="DROP TABLE test_00598"
