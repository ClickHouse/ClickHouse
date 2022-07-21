#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT --query_id="02368_$i" -q "insert into function hdfs('hdfs://localhost:12222/02368_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings hdfs_truncate_on_inser=1, toutput_format_parallel_formatting=1" 2> /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02368_') sync" > /dev/null

for i in $(seq 1 10);
do
    $CLICKHOUSE_CLIENT --query_id="02368_$i" -q "insert into function hdfs('hdfs://localhost:12222/02368_data_$i.jsonl') select range(number % 1000) from numbers(100000) settings hdfs_truncate_on_insert=1, output_format_parallel_formatting=0" 2> /dev/null &
done

sleep 2

$CLICKHOUSE_CLIENT -q "kill query where startsWith(query_id, '02368_') sync" > /dev/null
