#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

function check_log
{
${CLICKHOUSE_CLIENT} -nq "
system flush logs;

-- Check the number of spans with given trace id, to verify it was propagated.
select count(*)
    from system.opentelemetry_log
    where trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))
        and operation_name = 'query'
    ;

-- Check that the tracestate header was propagated. It must have exactly the
-- same non-empty value for all 'query' spans in this trace.
select count(distinct value)
    from system.opentelemetry_log
        array join attribute.names as name, attribute.values as value
    where
        trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))
        and operation_name = 'query'
        and name = 'tracestate'
        and length(value) > 0
    ;
"
}

# Generate some random trace id so that the prevous runs of the test do not interfere.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

# Check that the HTTP traceparent is read, and then passed through `remote` table function.
# We expect 4 queries, because there are two DESC TABLE queries for the shard.
# This is bug-ish, see https://github.com/ClickHouse/ClickHouse/issues/14228
${CLICKHOUSE_CURL} --header "traceparent: 00-$trace_id-0000000000000010-01" --header "tracestate: some custom state" "http://localhost:8123/" --get --data-urlencode "query=select 1 from remote('127.0.0.2', system, one)"

check_log

# With another trace id, check that clickhouse-client accepts traceparent, and
# that it is passed through URL table function. We expect two query spans, one
# for the initial query, and one for the HTTP query.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} --opentelemetry-traceparent "00-$trace_id-0000000000000020-02" --opentelemetry-tracestate "another custom state" --query "
    select * from url('http://127.0.0.2:8123/?query=select%201', CSV, 'a int')
"

check_log
