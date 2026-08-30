-- Tags: no-fasttest, no-msan
-- no-fasttest: the Fast test build has no embedded compiler (ENABLE_EMBEDDED_COMPILER defaults to
--              ENABLE_LIBRARIES, which that build sets to 0), so nothing is ever JIT-compiled and the
--              "same-unit shape is still compiled" assertion cannot hold.
-- no-msan: the MSan build also disables the embedded compiler (contrib/llvm-project-cmake/CMakeLists.txt),
--          so the same assertion cannot hold there.

SET session_timezone = 'UTC';
SET min_count_to_compile_expression = 0;

SELECT 'Date32 + DateTime64(0)', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 0))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 0))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date32 + DateTime64(3)', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date32 + DateTime64(6)', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 6))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 6))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date32 + DateTime64(9)', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 9))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 9))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date + DateTime64(3)', (SELECT groupArray(if(number % 2 = 0, toDate('2000-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate('2000-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date32 + DateTime', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date + DateTime', (SELECT groupArray(if(number % 2 = 0, toDate('2000-01-01'), toDateTime('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate('2000-01-01'), toDateTime('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Date32 + Date', (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDate('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDate('1970-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'branch order swapped', (SELECT groupArray(if(number % 2 = 0, toDateTime64('1970-01-01', 3), toDate32('1900-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDateTime64('1970-01-01', 3), toDate32('1900-01-01'))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'date as a column', (SELECT groupArray(if(number % 2 = 0, materialize(toDate32('1900-01-01')), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, materialize(toDate32('1900-01-01')), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'multiIf', (SELECT groupArray(multiIf(number % 3 = 0, toDate32('1900-01-01'), number % 3 = 1, toDateTime64('1980-01-01', 3), toDateTime64('1990-01-01', 3))) FROM numbers(3) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(multiIf(number % 3 = 0, toDate32('1900-01-01'), number % 3 = 1, toDateTime64('1980-01-01', 3), toDateTime64('1990-01-01', 3))) FROM numbers(3) SETTINGS compile_expressions = 0);

SELECT 'CASE', (SELECT groupArray(CASE WHEN number % 2 = 0 THEN toDate32('1900-01-01') ELSE toDateTime64('1970-01-01', 3) END) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(CASE WHEN number % 2 = 0 THEN toDate32('1900-01-01') ELSE toDateTime64('1970-01-01', 3) END) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'Nullable(Date32) + DateTime64(3)', (SELECT groupArray(if(number % 2 = 0, toNullable(toDate32('1900-01-01')), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toNullable(toDate32('1900-01-01')), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'post-1970 date', (SELECT groupArray(if(number % 2 = 0, toDate32('2000-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1)
    = (SELECT groupArray(if(number % 2 = 0, toDate32('2000-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 0);

SELECT 'absolute value', groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2) SETTINGS compile_expressions = 1;

-- The rows above compare compiled against interpreted, so they would all pass if compilation stopped
-- happening. These two pin which shapes compile. CompiledFunctionExecute counts executions of an
-- already-compiled node, so it does not care whether the global compiled cache was already warm.
SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDateTime64('1970-01-01', 3))) FROM numbers(2)
    SETTINGS compile_expressions = 1, log_comment = '04930_mixed_unit' FORMAT Null;

SELECT groupArray(if(number % 2 = 0, toDate32('1900-01-01'), toDate('1970-01-01'))) FROM numbers(2)
    SETTINGS compile_expressions = 1, log_comment = '04930_same_unit' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'mixed-unit shape is not compiled', ProfileEvents['CompiledFunctionExecute'] = 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04930_mixed_unit' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'same-unit shape is still compiled', ProfileEvents['CompiledFunctionExecute'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04930_same_unit' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC LIMIT 1;
