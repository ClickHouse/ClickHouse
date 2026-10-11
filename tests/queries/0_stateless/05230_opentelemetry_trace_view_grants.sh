#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# traceView is a wrapper over system.opentelemetry_span_log, and a table function is not a
# table: the outer SELECT checks no access on the span log. The fence is the queries the
# function runs itself, with the access of the caller: a user without SELECT on the span log
# must not see span attributes through any of the lookup paths (trace_id, query_id, cluster).

user="user_$CLICKHOUSE_TEST_UNIQUE_NAME"
query_id="query_$CLICKHOUSE_TEST_UNIQUE_NAME"

# Make sure the log table exists before writing to it, and use a trace id of this run only.
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(generateUUIDv4())")

# The root span is the 'query' span that `query_id = ...` looks up.
${CLICKHOUSE_CLIENT} -q "
    insert into system.opentelemetry_span_log
        (hostname, trace_id, span_id, parent_span_id, operation_name, kind, start_time_us, finish_time_us, finish_date, attribute)
    values
        ('h', '$trace_id', 1, 0, 'query', 'SERVER',   1000, 5000, today(), map('clickhouse.query_id', '$query_id', 'secret', 'value')),
        ('h', '$trace_id', 2, 1, 'child', 'INTERNAL', 2000, 3000, today(), map())
"
${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"

${CLICKHOUSE_CLIENT} -q "drop user if exists $user"
${CLICKHOUSE_CLIENT} -q "create user $user"

echo "=== without SELECT on the span log: every path is denied ==="
${CLICKHOUSE_CLIENT} --user "$user" -q "select attribute from traceView('$trace_id')" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "$user" -q "select attribute from traceView(query_id = '$query_id')" 2>&1 | grep -m1 -o "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --user "$user" -q "select attribute from traceView('$trace_id', 40, 'test_cluster_two_shards')" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "=== with SELECT on the span log: trace_id and query_id work, the cluster path also needs REMOTE ==="
${CLICKHOUSE_CLIENT} -q "grant select on system.opentelemetry_span_log to $user"
${CLICKHOUSE_CLIENT} --user "$user" -q "select span, attribute['secret'] from traceView('$trace_id') format TSV"
${CLICKHOUSE_CLIENT} --user "$user" -q "select span, attribute['secret'] from traceView(query_id = '$query_id') format TSV"
${CLICKHOUSE_CLIENT} --user "$user" -q "select attribute from traceView('$trace_id', 40, 'test_cluster_two_shards')" 2>&1 | grep -m1 -o "ACCESS_DENIED"

echo "=== with REMOTE too: the cluster path works ==="
${CLICKHOUSE_CLIENT} -q "grant remote on *.* to $user"
${CLICKHOUSE_CLIENT} --user "$user" -q "select span, attribute['secret'] from traceView('$trace_id', 40, 'test_cluster_two_shards') format TSV"

${CLICKHOUSE_CLIENT} -q "drop user $user"
