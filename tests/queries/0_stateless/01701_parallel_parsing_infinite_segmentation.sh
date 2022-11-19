#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "create table insert_big_json(a String, b String) engine=MergeTree() order by tuple()";

python3 -c "[print('{{\"a\":\"{}\", \"b\":\"{}\"'.format('clickhouse'* 1000000, 'dbms' * 1000000)) for i in range(10)]; [print('{{\"a\":\"{}\", \"b\":\"{}\"}}'.format('clickhouse'* 100000, 'dbms' * 100000)) for i in range(10)]" 2>/dev/null  | ${CLICKHOUSE_CLIENT} --input_format_parallel_parsing=1 --max_memory_usage=0 -q "insert into insert_big_json FORMAT JSONEachRow" 2>&1 | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL" ||:

${CLICKHOUSE_CLIENT} -q "drop table insert_big_json"
