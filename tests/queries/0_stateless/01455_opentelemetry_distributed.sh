#!/usr/bin/env bash
set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

function check_log
{
${CLICKHOUSE_CLIENT} --format=JSONEachRow -nq "
system flush logs;

-- Check the number of query spans with given trace id, to verify it was
-- propagated.
select count(*) "'"'"total spans"'"'",
        uniqExact(span_id) "'"'"unique spans"'"'",
        uniqExactIf(parent_span_id, parent_span_id != 0)
            "'"'"unique non-zero parent spans"'"'"
    from system.opentelemetry_span_log
    where trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))
        and operation_name = 'query'
    ;

-- Also check that the initial query span in ClickHouse has proper parent span.
select count(*) "'"'"initial query spans with proper parent"'"'"
    from
        (select *, attribute_name, attribute_value
            from system.opentelemetry_span_log
                array join attribute.names as attribute_name,
                    attribute.values as attribute_value) o
        join system.query_log on query_id = o.attribute_value
    where trace_id = reinterpretAsUUID(reverse(unhex('$trace_id')))
        and operation_name = 'query'
        and parent_span_id = reinterpretAsUInt64(unhex('73'))
        and o.attribute_name = 'clickhouse.query_id'
        and is_initial_query
        and type = 'QueryFinish'
    ;

-- Check that the tracestate header was propagated. It must have exactly the
-- same non-empty value for all 'query' spans in this trace.
select uniqExact(value) "'"'"unique non-empty tracestate values"'"'"
    from system.opentelemetry_span_log
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

# Check that the HTTP traceparent is read, and then passed through `remote`
# table function. We expect 4 queries -- one initial, one SELECT and two
# DESC TABLE. Two DESC TABLE instead of one looks like a bug, see the issue:
# https://github.com/ClickHouse/ClickHouse/issues/14228
${CLICKHOUSE_CURL} \
    --header "traceparent: 00-$trace_id-0000000000000073-01" \
    --header "tracestate: some custom state" "http://127.0.0.2:8123/" \
    --get \
    --data-urlencode "query=select 1 from remote('127.0.0.2', system, one) format Null"

check_log

# With another trace id, check that clickhouse-client accepts traceparent, and
# that it is passed through URL table function. We expect two query spans, one
# for the initial query, and one for the HTTP query.
echo "===native==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --opentelemetry-tracestate "another custom state" \
    --query "select * from url('http://127.0.0.2:8123/?query=select%201%20format%20Null', CSV, 'a int')"

check_log

# Test sampled tracing. The traces should be started with the specified
# probability, only for initial queries.
echo "===sampled==="
query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

for i in {1..200}
do
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry_start_trace_probability=0.1 \
        --query_id "$query_id-$i" \
        --query "select 1 from remote('127.0.0.2', system, one) format Null" \
        &

    # clickhouse-client is slow to start (initialization of DateLUT), so run
    # several clients in parallel, but not too many.
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
    select if(c > 1 and c < 50, 'OK', 'fail: ' || toString(c))
    from system.opentelemetry_span_log
        array join attribute.names as name, attribute.values as value
    where name = 'clickhouse.query_id'
        and operation_name = 'query'
        and parent_span_id = 0  -- only account for the initial queries
        and value like '$query_id-%'
    ;
"
