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
        and name = 'clickhouse.tracestate'
        and length(value) > 0
    ;
"
}

# Generate some random trace id so that the prevous runs of the test do not interfere.
echo "===http==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

# Check that the HTTP traceparent is read, and then passed through `remote` table function.
# We expect 4 queries, because there are two DESC TABLE queries for the shard.
# This is bug-ish, see https://github.com/ClickHouse/ClickHouse/issues/14228
${CLICKHOUSE_CURL} \
    --header "traceparent: 00-$trace_id-0000000000000010-01" \
    --header "tracestate: some custom state" "http://localhost:8123/" \
    --get \
    --data-urlencode "query=select 1 from remote('127.0.0.2', system, one)"

check_log

# With another trace id, check that clickhouse-client accepts traceparent, and
# that it is passed through URL table function. We expect two query spans, one
# for the initial query, and one for the HTTP query.
echo "===native==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000020-02" \
    --opentelemetry-tracestate "another custom state" \
    --query "select * from url('http://127.0.0.2:8123/?query=select%201', CSV, 'a int')"

check_log

# Test sampled tracing. The traces should be started with the specified probability,
# only for initial queries.
echo "===sampled==="
query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

for i in {1..200}
do
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry_start_trace_probability=0.1 \
        --query_id "$query_id-$i" \
        --query "select 1 from remote('127.0.0.2', system, one) format Null" \
        &

    # clickhouse-client is slow to start, so run them in parallel, but not too
    # much.
    if [[ $((i % 10)) -eq 0 ]]
    then
        wait
    fi
done
wait

${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "
    with count(*) as c
    -- expect 200 * 0.1 = 20 sampled events on average
    select if(c > 10 and c < 30, 'OK', 'fail: ' || toString(c))
    from system.opentelemetry_log
        array join attribute.names as name, attribute.values as value
    where name = 'clickhouse.query_id'
        and operation_name = 'query'
        and parent_span_id = 0  -- only account for the initial queries
        and value like '$query_id-%'
    ;
"
