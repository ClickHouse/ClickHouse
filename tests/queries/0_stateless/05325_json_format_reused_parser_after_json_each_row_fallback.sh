#!/usr/bin/env bash

# A `JSON` input format falls back to parsing as `JSONEachRow` when the input is not the
# `{"meta": ..., "data": [...]}` envelope. The fallback must not stick to the format object when
# it is reused for the next input, as asynchronous inserts and streaming engines do: a canonical
# envelope after a fallback input must still parse.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `async_insert_max_query_number` = 2 flushes both inserts as one batch, parsed by one format object.
url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_ms=600000&async_insert_max_query_number=2&async_insert_use_adaptive_busy_timeout=0&async_insert_deduplicate=0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_reused_parser"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_reused_parser (a UInt8) ENGINE = MergeTree ORDER BY a"

# The entries of a batch are parsed in queue order, so queue the fallback input first.
${CLICKHOUSE_CURL} -sS "$url" --data-binary 'INSERT INTO t_json_reused_parser FORMAT JSON {"a": 1}' &

while [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.asynchronous_inserts WHERE database = currentDatabase() AND table = 't_json_reused_parser'") == 0 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CURL} -sS "$url" --data-binary 'INSERT INTO t_json_reused_parser FORMAT JSON {"meta": [{"name": "a", "type": "UInt8"}], "data": [{"a": 2}], "rows": 1}' &

wait

${CLICKHOUSE_CLIENT} -q "SELECT a FROM t_json_reused_parser ORDER BY a"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_reused_parser"
