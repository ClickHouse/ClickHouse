#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e -o pipefail

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS test.test"
$CLICKHOUSE_CURL -sS -d 'CREATE TABLE test.test ENGINE = Memory AS SELECT 1' $CLICKHOUSE_URL
$CLICKHOUSE_CLIENT --query="SELECT * FROM test.test"
$CLICKHOUSE_CLIENT --query="DROP TABLE test.test"
