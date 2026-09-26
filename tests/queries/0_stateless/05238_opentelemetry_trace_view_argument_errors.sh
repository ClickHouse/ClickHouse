#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every malformed argument of traceView is reported by the function itself, in its own words,
# and points at the actual mistake. The spans are written to the log directly.

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

echo "=== a positional argument after a named one is not counted twice ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView(timeline_width = 20, '$trace_id')" 2>&1 | grep -m1 -o "positional argument .* after a named argument" | sed "s/'[^']*'/'<trace id>'/"

echo "=== the usage message does not present query_id as positional ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView()" 2>&1 | grep -m1 -o "or query_id = '...' in place of trace_id"

echo "=== an unknown cluster fails before the internal query ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('$trace_id', 40, 'no_such_cluster')" 2>&1 | grep -m1 -o "Requested cluster 'no_such_cluster' not found"

echo "=== a malformed trace id is reported by the function ==="
${CLICKHOUSE_CLIENT} -q "select span from traceView('not-a-uuid')" 2>&1 | grep -m1 -o "cannot parse 'not-a-uuid' as a trace_id UUID"

echo "=== the structure is static: CREATE TABLE AS traceView ==="
${CLICKHOUSE_CLIENT} -q "create table trace_view_copy as traceView('$trace_id')"
${CLICKHOUSE_CLIENT} -q "select count(), countIf(span like '%child%') from trace_view_copy format TSV"
${CLICKHOUSE_CLIENT} -q "drop table trace_view_copy"
