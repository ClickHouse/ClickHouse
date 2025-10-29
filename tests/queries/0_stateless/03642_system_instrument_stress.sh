#!/usr/bin/env bash
# Tags: use_xray, no-parallel, no-fasttest
# no-parallel: avoid other tests interfering with the global system.xray_instrumentation table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"

query_id_prefix="${CLICKHOUSE_DATABASE}"

statements=(
    "SELECT * FROM system.xray_instrumentation FORMAT NULL"
    "SELECT * FROM system.xray_instrumentation_profiling_log FORMAT NULL"
    "SYSTEM INSTRUMENT REMOVE ALL"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG ENTRY 'entry log'"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG EXIT 'exit log'"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` SLEEP ENTRY 0"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` SLEEP EXIT 0"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` PROFILE"
    "SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` PROFILE"
)

statements_nr=${#statements[@]}

function send_requests()
{
    for i in $(seq 1 1000); do
        query_id="${query_id_prefix}_${i}_${RANDOM}"
        statement=${statements[$(($RANDOM % $statements_nr))]}
        $CLICKHOUSE_CLIENT --query-id=$query_id -q "$statement" 2> /dev/null >&1
    done
}

# Send in parallel a bunch of queries to ensure nothing breaks.
# We don't care about the percise result since it depends on timing and the queries selected.
for i in $(seq 1 $(nproc)); do
    send_requests &
done

wait

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SYSTEM FLUSH LOGS system.text_log, xray_instrumentation_profiling_log;
    SELECT count() >= 1 FROM system.text_log WHERE event_date >= yesterday() AND query_id ILIKE '$query_id_prefix%';
    SELECT count() >= 1 FROM system.xray_instrumentation_profiling_log WHERE event_date >= yesterday() AND query_id ILIKE '$query_id_prefix%' AND function_name ILIKE 'QueryMetricLog%';
"""
