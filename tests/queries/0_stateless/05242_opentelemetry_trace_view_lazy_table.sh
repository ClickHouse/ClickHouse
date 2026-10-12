#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table created `AS traceView(...)` must not read the span log when it is created or attached:
# the table function is re-executed whenever the table is loaded (a server start, a `Replicated`
# database recovering a replica), on threads that cannot run a query and at a time when the spans
# may be gone. The span log is read when the table is read, so the table always shows the log as
# it is at that moment. The spans are written to the log directly.

# Make sure the log table exists, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

echo "=== the table is created before the trace has any spans ==="
${CLICKHOUSE_CLIENT} -q "create table trace_view_lazy as traceView('$trace_id')"
${CLICKHOUSE_CLIENT} -q "select count() from trace_view_lazy" 2>&1 | grep -m1 -o "No spans found for trace_id"

echo "=== the spans are read when the table is read ==="
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'root',  'INTERNAL', 1000, 5000, today(), map()),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 2000, 3000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
${CLICKHOUSE_CLIENT} -q "select span, duration_us from trace_view_lazy format TSV"

echo "=== attaching the table again does not read the span log ==="
${CLICKHOUSE_CLIENT} -q "detach table trace_view_lazy"
${CLICKHOUSE_CLIENT} -q "attach table trace_view_lazy"
${CLICKHOUSE_CLIENT} -q "select count() from trace_view_lazy format TSV"

${CLICKHOUSE_CLIENT} -q "drop table trace_view_lazy"
