#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A question for curious reader:
# How to break shell pipeline as soon as 5 lines are found in the following command:
# ./my-program | head -n5
# When I tried to do it, pipeline was not actively terminated,
#  unless the my-program will try to output a thousand more lines overflowing pipe buffer and terminating with Broken Pipe.
# But if my program just output 5 (or slightly more) lines and hang up, the pipeline is not terminated.

function test()
{
    timeout 5 ${CLICKHOUSE_LOCAL} --max_execution_time 10 --max_rows_to_read 0 --query "
        SELECT DISTINCT number % 5 FROM system.numbers" ||:
    echo -e '---'
    timeout 5 ${CLICKHOUSE_CURL} -sS --no-buffer "${CLICKHOUSE_URL}&max_execution_time=10&max_rows_to_read=0" --data-binary "
        SELECT DISTINCT number % 5 FROM system.numbers" ||:
    echo -e '---'
}

# The test depends on timeouts. And there is a chance that under high system load the query
# will not be able to finish in 5 seconds (this will lead to test flakiness).
# Let's check that is will be able to show the expected result at least once.
while true; do
    [[ $(test) == $(echo -ne "0\n1\n2\n3\n4\n---\n0\n1\n2\n3\n4\n---\n") ]] && break
    sleep 1
done
