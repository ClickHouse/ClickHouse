#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# A question for curious reader:
# How to break shell pipeline as soon as 5 lines are found in the following command:
# ./my-program | head -n5
# When I tried to do it, pipeline was not actively terminated,
#  unless the my-program will try to output a thousand more lines overflowing pipe buffer and terminating with Broken Pipe.
# But if my program just output 5 (or slightly more) lines and hang up, the pipeline is not terminated.

timeout 5 ${CLICKHOUSE_LOCAL} --max_execution_time 10 --query "SELECT DISTINCT number % 5 FROM system.numbers" ||:
echo '---'
timeout 5 ${CLICKHOUSE_CURL} -sS --no-buffer "${CLICKHOUSE_URL}&max_execution_time=10" --data-binary "SELECT DISTINCT number % 5 FROM system.numbers" ||:
echo '---'
