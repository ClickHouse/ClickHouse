#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# traceView only reads system.opentelemetry_span_log, so it must work under readonly = 1,
# which forbids CREATE TEMPORARY TABLE: the grant every table function needs unless it is
# registered as read-only. The span is written to the log directly to keep the trace small.

# Make sure the log table exists before writing to it, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'root',  'INTERNAL', 1000, 5000, today(), map()),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 2000, 3000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

echo "=== readonly = 1 ==="
${CLICKHOUSE_CLIENT} --readonly 1 -q "select span, duration_us from traceView('$trace_id') format TSV"
