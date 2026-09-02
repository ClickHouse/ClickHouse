#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_FORMAT --query="DESCRIBE file('data.csv')"
$CLICKHOUSE_FORMAT --query="DESCRIBE TABLE table"
$CLICKHOUSE_FORMAT --query="DESC file('data.csv')"

