#!/usr/bin/env bash
# Tags: distributed, no-flaky-check

set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function check_log
{
    # A span's TracingContextHolder is logged to opentelemetry_span_log only when
    # the corresponding handler/query fully exits, which for the gateway happens
    # after the response was already sent to the client. The query spans and the
    # gateway spans they descend from are flushed by independent background
    # threads, so right after the client call returns an arbitrary subset may be
    # in the table. Poll until the exact quantities the assertions below read are
    # present: the total number of 'query' spans ($1) and the number of initial
    # query spans with a proper parent ($2). Waiting on a proxy (e.g. "any span
    # with parent 0x73 exists") breaks when only one of the two gateway-parented
    # spans has flushed, leaving 'total spans' < 3 or 'initial ... proper parent'
    # short. See issues 67108, 93452 and the amd_tsan flakiness this revision fixes.
    local _expected_query_spans="$1"
    local _expected_initial_with_parent="$2"
    local _query_spans=0 _initial_with_parent=0 _flushed=0
    # 20 tries spaced 1s apart: give the background flush threads ~20s to land
    # the spans without hammering the server with a tight flush loop.
    for _retry in {1..20}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        _counts=$(${CLICKHOUSE_CLIENT} -q "
            select
                countIf(operation_name = 'query'),
                countIf(operation_name = 'query' and parent_span_id in (
                    select span_id from system.opentelemetry_span_log
                    where finish_date >= yesterday()
                      AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
                      and parent_span_id = reinterpretAsUInt64(unhex('73'))))
            from system.opentelemetry_span_log
            where finish_date >= yesterday()
              AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        ")
        _query_spans=$(echo "$_counts" | cut -f1)
        _initial_with_parent=$(echo "$_counts" | cut -f2)
        if [[ "$_query_spans" -ge "$_expected_query_spans" && "$_initial_with_parent" -ge "$_expected_initial_with_parent" ]]; then
            _flushed=1
            break
        fi
        sleep 1
    done
    # Fail loudly if the spans never showed up, instead of falling through to the
    # assertions and reporting a cryptic "total spans: 2".
    if [[ "$_flushed" -ne 1 ]]; then
        echo "check_log: opentelemetry spans not flushed within 20s:" \
             "query_spans=$_query_spans (expected >= $_expected_query_spans)," \
             "initial_query_spans_with_parent=$_initial_with_parent" \
             "(expected >= $_expected_initial_with_parent)" >&2
        exit 1
    fi

${CLICKHOUSE_CLIENT} --format=JSONEachRow -q "
set enable_analyzer = 1;
-- Spans are already flushed by the retry loop above.

-- Show queries sorted by start time.
select attribute['db.statement'] as query,
       attribute['clickhouse.query_status'] as status,
       attribute['clickhouse.tracestate'] as tracestate,
       1 as sorted_by_start_time
    from system.opentelemetry_span_log
    where finish_date >= yesterday() AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'query'
    order by start_time_us
    ;

-- Show queries sorted by finish time.
select attribute['db.statement'] as query,
       attribute['clickhouse.query_status'] as query_status,
       attribute['clickhouse.tracestate'] as tracestate,
       1 as sorted_by_finish_time
    from system.opentelemetry_span_log
    where finish_date >= yesterday() AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'query'
    order by finish_time_us
    ;

-- Check the number of query spans with given trace id, to verify it was
-- propagated.
select count(*) "'"'"total spans"'"'",
        uniqExact(span_id) "'"'"unique spans"'"'",
        uniqExactIf(parent_span_id, parent_span_id != 0)
            "'"'"unique non-zero parent spans"'"'"
    from system.opentelemetry_span_log
    where finish_date >= yesterday() AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'query'
    ;

-- Also check that the initial query span in ClickHouse has proper parent span.
-- the first span should be child of input trace context
-- the 2nd span should be the 'query' span
select count(*) "'"'"initial query spans with proper parent"'"'"
    from system.opentelemetry_span_log
    where finish_date >= yesterday() AND
        trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'query'
        and parent_span_id in (
           select span_id from system.opentelemetry_span_log where finish_date >= yesterday() AND trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) and parent_span_id = reinterpretAsUInt64(unhex('73'))
        )
    ;

-- Check that the tracestate header was propagated. It must have exactly the
-- same non-empty value for all 'query' spans in this trace.
select uniqExact(value) "'"'"unique non-empty tracestate values"'"'"
    from system.opentelemetry_span_log
        array join mapKeys(attribute) as name,  mapValues(attribute) as value
    where finish_date >= yesterday() AND
        trace_id = UUIDNumToString(toFixedString(unhex('$trace_id'), 16))
        and operation_name = 'query'
        and name = 'clickhouse.tracestate'
        and length(value) > 0
    ;
"
}

# Generate some random trace id so that the prevous runs of the test do not interfere.
echo "===http==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4())))) settings enable_analyzer = 1")

# Check that the HTTP traceparent is read, and then passed through `remote`
# table function. We expect 4 queries -- one initial, one SELECT and two
# DESC TABLE. Two DESC TABLE instead of one looks like a bug, see the issue:
# https://github.com/ClickHouse/ClickHouse/issues/14228
${CLICKHOUSE_CURL} \
    --header "traceparent: 00-$trace_id-0000000000000073-01" \
    --header "tracestate: some custom state" "$CLICKHOUSE_URL" \
    --get \
    --data-urlencode "query=select 1 from remote('127.0.0.2', system, one) settings enable_analyzer = 1 format Null"

# 3 'query' spans (initial + remote DESC TABLE + remote rewritten SELECT), 2 of
# which descend from the input trace context (0x73): the initial query and the
# remote SELECT.
check_log 3 2

# With another trace id, check that clickhouse-client accepts traceparent, and
# that it is passed through URL table function. We expect two query spans, one
# for the initial query, and one for the HTTP query.
echo "===native==="
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --opentelemetry-tracestate "another custom state" \
    --query "select * from url('http://127.0.0.2:8123/?query=select%201%20format%20Null', CSV, 'a int')"

# 3 'query' spans (initial + two for the HTTP url() call), 1 of which descends
# from the input trace context (0x73): the initial query.
check_log 3 1

# Test sampled tracing. The traces should be started with the specified
# probability, only for initial queries.
echo "===sampled==="
query_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

for i in {1..40}
do
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry_start_trace_probability=0.5 \
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

${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
${CLICKHOUSE_CLIENT} -q "
    -- expect 40 * 0.5 = 20 sampled events on average;
    -- probability of getting 0, 1, 39, or 40 sampled events: 82/2^40 = 1 in 13.4 B runs;
    -- if there are 10k tests run 1k times per day, that's a false positive every 3.7 years
    select if(2 <= count() and count() <= 38, 'OK', 'Fail')
    from system.opentelemetry_span_log
    where finish_date >= yesterday() AND operation_name = 'query'
        and attribute['clickhouse.query_id'] like '$query_id-%'
    ;
"
