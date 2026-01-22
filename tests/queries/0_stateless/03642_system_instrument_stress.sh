#!/usr/bin/env bash
# Tags: use-xray, no-parallel, no-fasttest, no-llvm-coverage
# no-parallel: avoid other tests interfering with the global system.instrumentation table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
}

trap cleanup EXIT

# In this test, we just care of sending several queries in parallel so make sure the server
# can handle them without race conditions that may cause a crash.

query_id_prefix="${CLICKHOUSE_DATABASE}"

statements=(
    "SELECT * FROM system.instrumentation FORMAT NULL"
    "SELECT count() FROM system.symbols WHERE function_id IS NOT NULL FORMAT NULL SETTINGS allow_introspection_functions=1"
    "SELECT * FROM system.trace_log WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 1 MINUTE AND trace_type = 'Instrumentation' FORMAT NULL"
    "SYSTEM INSTRUMENT REMOVE ALL"
    "SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation LIMIT 2)"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'entry log'"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'exit log'"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' SLEEP ENTRY 0"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' SLEEP EXIT 0"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' SLEEP ENTRY 0 0.01"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' SLEEP EXIT 0 0.01"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE"
    "SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' PROFILE"
)

statements_nr=${#statements[@]}

function send_requests()
{
    for i in $(seq 1 100); do
        query_id="${query_id_prefix}_${i}_${RANDOM}"
        statement=${statements[$(($RANDOM % $statements_nr))]}
        $CLICKHOUSE_CLIENT --query-id=$query_id -q "$statement" 2> /dev/null >&1
    done
}

# Send in parallel a bunch of queries to ensure nothing breaks.
# We don't care about the precise result since it depends on timing and the queries selected.
for i in $(seq 1 $(nproc)); do
    send_requests &
done

wait

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.text_log, system.trace_log;
    SELECT count() >= 1 FROM system.text_log WHERE event_date >= yesterday() AND query_id ILIKE '$query_id_prefix%';
    SELECT count() >= 1 FROM system.trace_log WHERE event_date >= yesterday() AND query_id ILIKE '$query_id_prefix%' AND function_name ILIKE '%QueryMetricLog%' AND arrayExists(x -> x LIKE '%dispatchHandler%', symbols);
"""
