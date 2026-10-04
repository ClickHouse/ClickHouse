#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A span whose finish_time_us is before its start_time_us is a clock artifact; traceView
# treats such a span as instantaneous. The bounds of the whole trace must follow the same
# rule: in a trace where every span is inverted, the largest raw finish is below the smallest
# start, and the trace duration would underflow, collapsing every timeline to the left edge.
# The spans are written to the log directly: such shapes cannot be produced by a real query.

# Make sure the log table exists before writing to it, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

# The trace spans 3000..5000: the child starts first, the root starts last.
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'root',  'INTERNAL', 5000, 1000, today(), map()),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 3000, 2000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

# Both spans are instantaneous; the root sits at the right edge of the timeline, the child at
# the left edge.
${CLICKHOUSE_CLIENT} -q "
    select span, start_offset_us, duration_us, self_pct, timeline
    from traceView('$trace_id', 8)
    format TSV
"
