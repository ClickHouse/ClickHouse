#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n --query="SELECT sum(number * 0) FROM numbers(10000000000) SETTINGS partial_result_on_first_cancel=true;" &
pid=$!
sleep 2
kill -SIGINT $pid
wait $pid
