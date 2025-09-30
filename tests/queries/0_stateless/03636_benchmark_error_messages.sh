#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --bad-option 2>&1
$CLICKHOUSE_BENCHMARK --timelimit "bad value" 2>&1
$CLICKHOUSE_BENCHMARK --user "invalid user" --password "invalid password" --query "SELECT 1" 2>&1