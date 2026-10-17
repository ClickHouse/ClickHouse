#!/usr/bin/env bash
# Tags: no-fasttest

# Async inserts of `JSONEachRow` append rows directly into the accumulated columns of the batch.
# A row that fails after writing a new `JSON` path and a new `Dynamic` type must be rolled back
# without affecting the rows of the other entries in the same flush.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `async_insert_max_query_number` = 3 flushes the three inserts as one batch; see `02015_async_inserts_2`.
url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1&async_insert_busy_timeout_ms=600000&async_insert_max_query_number=3&async_insert_use_adaptive_busy_timeout=0&async_insert_deduplicate=0&input_format_parallel_parsing=0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_partial_row"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_partial_row (j JSON, d Dynamic, x UInt8) ENGINE = MergeTree ORDER BY x"

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_json_partial_row FORMAT JSONEachRow
{"j": {"a": 1}, "d": 1, "x": 1}
{"j": {"a": 2}, "d": 2, "x": 2}' &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_json_partial_row FORMAT JSONEachRow
{"j": {"a": 3}, "d": 3, "x": 3}
{"j": {"b": "s"}, "d": [1, 2], "x": "bad"}' 2>&1 | grep -q -F "Exception" && echo "error" &

${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO t_json_partial_row FORMAT JSONEachRow
{"j": {"a": 4}, "d": "s", "x": 4}' &

wait

${CLICKHOUSE_CLIENT} -q "SELECT x, j.a, d, dynamicType(d) FROM t_json_partial_row ORDER BY x"
${CLICKHOUSE_CLIENT} -q "SELECT DISTINCT arraySort(JSONDynamicPaths(j)) FROM t_json_partial_row"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_partial_row' AND active"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_json_partial_row"
