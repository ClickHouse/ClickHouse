#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The span log is partitioned and ordered by finish_date, and trace_id is not in the key, so
# traceView scans the whole log unless the named arguments since/until bound finish_date.
# The window is inclusive and applies to both lookups: by trace_id and by query_id.
# The spans are written to the log directly, with finish dates a query cannot produce.

query_id="query_$CLICKHOUSE_TEST_UNIQUE_NAME"

# Make sure the log table exists before writing to it, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

# The root 'query' span finished ten days ago, the child today.
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'query', 'SERVER',   1000, 5000, today() - 10, map('clickhouse.query_id', '$query_id')),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 2000, 3000, today(),      map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

echo "=== no window: every span ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id') format TSV"

echo "=== since yesterday: the child only ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', since = toString(today() - 1)) format TSV"

echo "=== until five days ago: the root only ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', until = toString(today() - 5)) format TSV"

echo "=== inclusive bounds: both ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', since = toString(today() - 10), until = toString(today())) format TSV"

echo "=== an empty window says so ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', since = toString(today() + 1))" 2>&1 | grep -m1 -o "within the since/until window"

echo "=== query_id lookup honours the window ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView(query_id = '$query_id', since = toString(today() - 10)) format TSV"
${CLICKHOUSE_CLIENT} -q "select span from traceView(query_id = '$query_id', since = toString(today() - 1))" 2>&1 | grep -m1 -o "No trace found for query_id.*" | grep -m1 -o "within the since/until window"

echo "=== invalid arguments ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', since = 'yesterday')" 2>&1 | grep -m1 -o "cannot parse 'yesterday' as a YYYY-MM-DD date for since"
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', 40, 'test_cluster_two_shards', '2026-01-01')" 2>&1 | grep -m1 -o "argument 4 must be given by name"
