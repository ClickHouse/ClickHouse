#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# Generate some random trace id so that the prevous runs of the test do not interfere.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CURL} --header "traceparent: 00-$trace_id-0000000000000010-01" --header "tracestate: some custom state" "http://localhost:8123/" --get --data-urlencode "query=select 1 from remote('127.0.0.2', system, one)"

${CLICKHOUSE_CLIENT} -q "system flush logs"

# Check that the HTTP traceparent was read, and then passed to `remote` instance.
# We expect 4 queries, because there are two DESC TABLE queries for the shard.
# This is bug-ish, see https://github.com/ClickHouse/ClickHouse/issues/14228
${CLICKHOUSE_CLIENT} -q "select count(*) from system.opentelemetry_log where trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))"

# Check that the tracestate header was read and passed. Must have
# exactly the same value for all "query" spans in this trace.
${CLICKHOUSE_CLIENT} -q "
    select count(distinct attribute.values)
    from system.opentelemetry_log
        array join attribute.names, attribute.values
    where
        trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))
        and operation_name = 'query'
        and attribute.names = 'tracestate'
"


