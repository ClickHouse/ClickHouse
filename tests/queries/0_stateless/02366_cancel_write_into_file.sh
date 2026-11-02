#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


for i in $(seq 1 10);
do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=02366_$i" -d "insert into function file(currentDatabase() || '_02366_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings output_format_parallel_formatting=1" >& /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02366_') sync" > /dev/null 2>&1

for i in $(seq 1 10);
do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=02366_$i" -d "insert into function file(currentDatabase() || '_02366_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings output_format_parallel_formatting=0" >& /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02366_') sync" > /dev/null 2>&1
