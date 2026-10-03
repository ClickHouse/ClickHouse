#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `cluster`, traceView registers a Distributed table over the span logs of the replicas as a
# temporary table of a private copy of the caller's context. That copy carries the caller's own
# temporary tables, so the name must not be one the caller could have taken.
# The spans are written to the log directly.

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

echo "=== a temporary table named like the internal one does not get in the way ==="
${CLICKHOUSE_CLIENT} -q "
    create temporary table _trace_view_span_log (x UInt8);
    select span, duration_us from traceView('$trace_id', cluster = 'test_shard_localhost') format TSV;
    select count() from _trace_view_span_log format TSV;
"

echo "=== two cluster reads in one query do not collide ==="
${CLICKHOUSE_CLIENT} -q "
    select count() from traceView('$trace_id', cluster = 'test_shard_localhost') as a,
                       traceView('$trace_id', cluster = 'test_shard_localhost') as b format TSV
"
