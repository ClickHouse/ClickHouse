#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# traceView builds its result with internal queries over the span log that run with the caller's
# context. The caller's settings that shape its own result - filters, limit and offset, result size
# limits - must apply to the final traceView output only: applied to the internal queries they would
# cut the spans read, or fail on the `span` column that only the output has.

${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'query',   'SERVER',   1000, 9000, today(), map()),
        ('h', '$trace_id', 2, 1, 'child_a', 'INTERNAL', 2000, 4000, today(), map()),
        ('h', '$trace_id', 3, 1, 'child_b', 'INTERNAL', 5000, 8000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

echo "=== additional_result_filter filters the output, the whole trace is read ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id') order by start_offset_us settings additional_result_filter = 'span LIKE ''%child%'''"

echo "=== the filter setting filters the output ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id') order by start_offset_us settings filter = 'span LIKE ''%child%'''"

echo "=== max_result_rows counts the output rows, not the spans read ==="
${CLICKHOUSE_CLIENT} -q "select count() from traceView('$trace_id') settings max_result_rows = 1, result_overflow_mode = 'throw'"

echo "=== a session-level limit and offset shape the output only ==="
${CLICKHOUSE_CLIENT} -q "set limit = 1; select count() from traceView('$trace_id')"
${CLICKHOUSE_CLIENT} -q "set offset = 1; select span from traceView('$trace_id') order by start_offset_us"

echo "=== the resource limits still apply to the spans read ==="
${CLICKHOUSE_CLIENT} -q "select count() from traceView('$trace_id') settings max_rows_to_read = 1" 2>&1 | grep -m1 -o "TOO_MANY_ROWS"
