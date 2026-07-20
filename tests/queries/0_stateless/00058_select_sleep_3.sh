#!/usr/bin/env bash
# Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings
# Test is used by ci/tests

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

timer=3

$CLICKHOUSE_CLIENT --query="SELECT sleep($timer)" &
$CLICKHOUSE_CLIENT --query="SELECT sleep($timer)" &
$CLICKHOUSE_CLIENT --query="SELECT sleep($timer)" &
$CLICKHOUSE_CLIENT --query="SELECT sleep($timer)" &
$CLICKHOUSE_CLIENT --query="SELECT sleep($timer)" &
$CLICKHOUSE_CLIENT --query="SELECT 1" &
wait
