#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

output=$(${CLICKHOUSE_CLIENT} -t -q "SELECT sleepEachRow(2) FORMAT Null" 2>&1)
{ number=$(echo "$output" | grep -o "^[0-9]"); [[ -n "$number" && "$number" -ge 2 ]] && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }

output=$(${CLICKHOUSE_CLIENT} --time -q "SELECT sleepEachRow(2) FORMAT Null" 2>&1)
{ number=$(echo "$output" | grep -o "^[0-9]"); [[ -n "$number" && "$number" -ge 2 ]] && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }

output=$(${CLICKHOUSE_CLIENT} --memory-usage -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1)
{ echo "$output" | grep -q "^[0-9]\+$" && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }

output=$(${CLICKHOUSE_CLIENT} --memory-usage=none -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1)
echo -n "$output" # expected no output

output=$(${CLICKHOUSE_CLIENT} --memory-usage=default -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1)
{ echo "$output" | grep -q "^[0-9]\+$" && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }

output=$(${CLICKHOUSE_CLIENT} --memory-usage=readable -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1)
{ echo "$output" | grep -q "^[0-9].*B$" && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }

output=$(${CLICKHOUSE_CLIENT} --memory-usage=unknown -q "SELECT sum(number) FROM numbers(10_000) FORMAT Null" 2>&1)
{ echo "$output" | grep -q "BAD_ARGUMENTS" && echo "Ok"; } || { echo "Fail"; echo "'$output'"; }
