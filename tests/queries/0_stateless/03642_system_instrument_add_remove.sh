#!/usr/bin/env bash
# Tags: use_xray, no-parallel, no-fasttest
# no-parallel: avoid other tests interfering with the global system.xray_instrumentation table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reading the instrumentation points forces the initial load of the symbols

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM INSTRUMENT REMOVE ALL;"
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT count() FROM system.xray_instrumentation;

    SYSTEM INSTRUMENT ADD \`DB::executeQuery\` LOG ENTRY 'my log in executeQuery';
    SELECT function_name, handler, entry_type, parameters FROM system.xray_instrumentation ORDER BY id ASC;
    SYSTEM INSTRUMENT ADD \`DB::executeQuery\` LOG ENTRY 'another log in executeQuery'; -- { serverError BAD_ARGUMENTS }
    SELECT function_name, handler, entry_type, parameters FROM system.xray_instrumentation ORDER BY id ASC;

    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG ENTRY 'my log in startQuery';
    SELECT function_name, handler, entry_type, parameters FROM system.xray_instrumentation ORDER BY id ASC;
"""

id=$($CLICKHOUSE_CLIENT -q "SELECT id FROM system.xray_instrumentation WHERE function_name = 'QueryMetricLog::startQuery';")

$CLICKHOUSE_CLIENT -q """
    SYSTEM INSTRUMENT REMOVE $id;
    SELECT function_name, handler, entry_type, parameters FROM system.xray_instrumentation ORDER BY id ASC;

    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG ENTRY 'my other in startQuery';
    SELECT function_name, handler, entry_type, parameters FROM system.xray_instrumentation ORDER BY id ASC;
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT count() FROM system.xray_instrumentation;
"""
