#!/usr/bin/env bash
# Tags: use-xray, no-parallel
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

$CLICKHOUSE_CLIENT -q "
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT '-- Empty table';
    SELECT count() FROM system.instrumentation;

    SELECT '-- Add one';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' LOG ENTRY 'my log in finishQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Adding the same one again';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' LOG ENTRY 'another log in finishQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Add another one';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'my log in startQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;
"

id=$($CLICKHOUSE_CLIENT -q "SELECT id FROM system.instrumentation WHERE function_name = 'QueryMetricLog::startQuery';")

$CLICKHOUSE_CLIENT -q "
    SELECT '-- Remove one specific id';
    SYSTEM INSTRUMENT REMOVE $id;
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Add 2 more';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'my other in startQuery';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::finishQuery' LOG EXIT 'my other in finishQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Remove the entries with entry_type = Exit';
    SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE entry_type = 'Exit');
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Remove with wrong arguments fails';
    SYSTEM INSTRUMENT REMOVE (SELECT id, function_id FROM system.instrumentation); -- { serverError BAD_ARGUMENTS }
    SYSTEM INSTRUMENT REMOVE (SELECT handler FROM system.instrumentation); -- { serverError BAD_ARGUMENTS }
    SYSTEM INSTRUMENT REMOVE 3.2; -- { clientError SYNTAX_ERROR }

    SELECT '-- Remove everything';
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT count() FROM system.instrumentation;
"

$CLICKHOUSE_CLIENT -q "
    SELECT '-- Add several functions that match';
    SYSTEM INSTRUMENT ADD 'executeQuery' LOG ENTRY 'my log in executeQuery';
    SELECT count() > 10, function_name, handler, entry_type FROM system.instrumentation WHERE symbol ILIKE '%executeQuery%' GROUP BY function_name, handler, entry_type;

    SELECT '-- Remove functions that match';
    SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'my log in startQuery';
    SYSTEM INSTRUMENT REMOVE 'unknown'; -- { serverError BAD_ARGUMENTS }
    SYSTEM INSTRUMENT REMOVE 'executeQuery';
    SELECT function_name, handler, entry_type, symbol, parameters FROM system.instrumentation ORDER BY id ASC;

    SELECT '-- Remove everything';
    SYSTEM INSTRUMENT REMOVE ALL;
    SELECT count() FROM system.instrumentation;
"
