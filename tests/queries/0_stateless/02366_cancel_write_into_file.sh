#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT --query_id="02366_$i" -q "insert into function file('02366_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings output_format_parallel_formatting=1" 2> /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02366_') sync" 2>&1 /dev/null
$CLICKHOUSE_CLIENT -q "select demangle(addressToSymbol(arrayJoin(trace))) from system.stack_trace where startsWith(query_id, '02366_')" 2>&1 /dev/null

for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT --query_id="02366_$i" -q "insert into function file('02366_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings output_format_parallel_formatting=0" 2> /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02366_') sync" 2>&1 /dev/null
$CLICKHOUSE_CLIENT -q "select demangle(addressToSymbol(arrayJoin(trace))) from system.stack_trace where startsWith(query_id, '02366_')" 2>&1 /dev/null

