#!/usr/bin/env bash
# Tags: use_xray, no-parallel, no-fasttest
# no-parallel: avoid other tests interfering with the global system.instrumentation table

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
    SELECT '-- Empty table';
    SELECT count() FROM system.instrumentation;

    SELECT '-- Add one entry';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` LOG ENTRY 'my log in finishQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Adding the same entry produces an error';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` LOG ENTRY 'another log in finishQuery'; -- { serverError BAD_ARGUMENTS }
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Add another entry';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG ENTRY 'my log in startQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;
"""

id=$($CLICKHOUSE_CLIENT -q "SELECT id FROM system.instrumentation WHERE function_name = 'QueryMetricLog::startQuery';")

$CLICKHOUSE_CLIENT -q """
    SELECT '-- Remove one specific id';
    SYSTEM INSTRUMENT REMOVE $id;
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Add 2 more entries';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::startQuery\` LOG EXIT 'my other in startQuery';
    SYSTEM INSTRUMENT ADD \`QueryMetricLog::finishQuery\` LOG EXIT 'my other in finishQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Remove the entries with entry_type = exit';
    SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE entry_type = 'exit');
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Remove everything';
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT count() FROM system.instrumentation;
"""
