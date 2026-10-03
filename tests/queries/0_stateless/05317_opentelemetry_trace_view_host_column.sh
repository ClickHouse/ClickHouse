#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `host` column of traceView is the `hostname` of the server that recorded the span. In a
# trace read from several nodes it tells the spans of the initiator from those of the remote
# nodes, which the span text alone does not. The spans are written to the log directly, with
# the hostnames of a fictitious cluster. A real span carries the FQDN of the server (`fqdn()`), as
# the span log writes it.

${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('initiator', '$trace_id', 1, 0, 'query',                        'INTERNAL', 1000, 9000, today(), map()),
        ('initiator', '$trace_id', 2, 1, 'DistributedPlanTask::dispatch', 'CLIENT',   2000, 3000, today(), map()),
        ('worker-1',  '$trace_id', 3, 2, 'DistributedPlanTask::execute',  'SERVER',   2500, 8000, today(), map()),
        ('initiator', '$trace_id', 4, 1, 'DistributedPlanTask::dispatch', 'CLIENT',   2100, 3100, today(), map()),
        ('worker-2',  '$trace_id', 5, 4, 'DistributedPlanTask::execute',  'SERVER',   2600, 7000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

echo "=== the host of every span ==="
${CLICKHOUSE_CLIENT} -q "select span, kind, host from traceView('$trace_id') format TSV"

echo "=== the spans of one node ==="
${CLICKHOUSE_CLIENT} -q "select span, host from traceView('$trace_id') where host = 'worker-2' format TSV"

echo "=== a real trace is recorded under the server's own FQDN ==="
trace_id_hex=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
real_trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(UUIDNumToString(toFixedString(unhex('$trace_id_hex'), 16)))")
${CLICKHOUSE_CLIENT} --opentelemetry-traceparent "00-$trace_id_hex-0000000000000073-01" -q "select 1 format Null"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
${CLICKHOUSE_CLIENT} -q "
    select if(count() > 0 and countIf(host != fqdn()) = 0, 'every span carries fqdn(): OK', 'every span carries fqdn(): FAIL')
    from traceView('$real_trace_id') format TSV
"
