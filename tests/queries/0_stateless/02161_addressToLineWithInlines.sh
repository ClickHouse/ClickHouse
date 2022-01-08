#! /bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug
# tags are copied from 00974_query_profiler.sql

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export CLICKHOUSE_DATABASE=system
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

MAX_FAILED_COUNT=10
MAX_RETRY_COUNT=10
log_comment="02161_testcase_$(date +'%s')"

check_exist_sql="SELECT count(), query_id FROM system.trace_log WHERE trace_type IN ('CPU', 'Real') AND query_id IN (
    SELECT query_id FROM query_log WHERE log_comment = '${log_comment}' ORDER BY event_time DESC LIMIT 1
) GROUP BY query_id"

declare exist_string_result
declare -A exist_result=([count]=0 [query_id]="")

function update_log_comment() {
    log_comment="02161_testcase_$(date +'%s')"
}


function flush_log() {
    ${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SYSTEM FLUSH LOGS'
}

function get_trace_count() {
    flush_log
    ${CLICKHOUSE_CLIENT} -q 'SELECT count() from system.trace_log';
}

function make_trace() {
    ${CLICKHOUSE_CLIENT} --query_profiler_cpu_time_period_ns=1000000 --allow_introspection_functions 1 -q "SELECT addressToLineWithInlines(arrayJoin(trace)) FROM system.trace_log SETTINGS log_comment='${log_comment}'"
}

function check_exist() {
    exist_string_result=$(${CLICKHOUSE_CLIENT} --log_queries=0 -q "${check_exist_sql}")
    exist_result[count]="$(echo "$exist_string_result" | cut -f 1)"
    exist_result[query_id]="$(echo "$exist_string_result" | cut -f 2)"
}

function get4fail() {
    ${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "SELECT addressToLineWithInlines(arrayJoin(trace)) FROM system.trace_log WHERE trace_type IN ('CPU', 'Real') AND query_id='$1'"
    ${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "SELECT addressToLine(arrayJoin(trace)) FROM system.trace_log WHERE trace_type IN ('CPU', 'Real') AND query_id='$1'"
}

function final_check_inlines() {
    final_check_sql="WITH
    address_list AS
    (
        SELECT DISTINCT arrayJoin(trace) AS address FROM system.trace_log WHERE query_id='$1'
    )
SELECT max(length(addressToLineWithInlines(address))) > 1 FROM address_list;"
    result="$(${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "$final_check_sql")"
    [[ "$result" == "1" ]]
}

function final_check() {
    final_check_sql="WITH
    address_list AS
    (
        SELECT DISTINCT arrayJoin(trace) AS address FROM system.trace_log WHERE query_id='$1'
    )
SELECT max(length(addressToLineWithInlines(address))) >= 1 FROM address_list;"
    result="$(${CLICKHOUSE_CLIENT} --allow_introspection_functions 1 -q "$final_check_sql")"
    [[ "$result" == "1" ]]
}

echo "CHECK: privilege"
${CLICKHOUSE_CURL} -sS "$CLICKHOUSE_URL" -d 'SELECT addressToLineWithInlines(1);' | grep -oF 'Code: 446.' || echo 'FAIL'

echo "CHECK: basic call"

# won't check inline because there is no debug symbol in some test env.
# e.g: https://s3.amazonaws.com/clickhouse-test-reports/33467/2081b43c9ee59615b2fd31c77390744b10eef61e/stateless_tests__release__wide_parts_enabled__actions_.html

flush_log
result=""
for ((i=0;i<MAX_RETRY_COUNT;i++)); do
    check_exist
    failed_count=$MAX_FAILED_COUNT
    while [[ ${exist_result[count]} -le 0 ]]; do
        ((failed_count=failed_count - 1))
        if [[ $failed_count -le 0 ]];then
            update_log_comment
            failed_count=$MAX_FAILED_COUNT
        fi
        make_trace > /dev/null
        flush_log
        sleep 1
        check_exist
    done
    if final_check "${exist_result[query_id]}";then
        result="Success"
        break
    fi
    update_log_comment
done

if final_check "${exist_result[query_id]}"; then
    result="Success"
else
    echo "query_id: ${exist_result[query_id]}, count: ${exist_result[count]}"
    get4fail "${exist_result[query_id]}"
fi
echo "$result"
