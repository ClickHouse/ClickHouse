-- A lazily executed `and` inside another short-circuit function, with an argument that is shared with another output
-- (so it is already executed on all rows), keeps executing its first argument first, unless reordering is allowed.

-- `blockSize` is not deterministic in the scope of the query, so it must be executed on the rows selected by `if`.
SELECT number, if(number % 3 = 0, blockSize() >= 3 AND number % 2 = 0, 0), number % 2 = 0
FROM numbers(10)
SETTINGS max_block_size = 10, short_circuit_function_evaluation = 'force_enable',
    short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0;

SELECT number, if(number % 3 = 0, blockSize() >= 3 AND number % 2 = 0, 0), number % 2 = 0
FROM numbers(10)
SETTINGS max_block_size = 10, short_circuit_function_evaluation = 'enable',
    short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0;

-- `sleepEachRow` has observable side effects, so it is executed on all the rows selected by `if`.
SELECT sum(if(number % 3 = 0, sleepEachRow(0.001) = 0 AND number % 2 = 0, 0)), sum(number % 2 = 0)
FROM numbers(100)
SETTINGS max_block_size = 10, short_circuit_function_evaluation = 'enable',
    short_circuit_function_evaluation_reorder_arguments = 0, compile_expressions = 0, log_comment = '05394_sleep_0';

SELECT sum(if(number % 3 = 0, sleepEachRow(0.001) = 0 AND number % 2 = 0, 0)), sum(number % 2 = 0)
FROM numbers(100)
SETTINGS max_block_size = 10, short_circuit_function_evaluation = 'enable',
    short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0, log_comment = '05394_sleep_1';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SleepFunctionMicroseconds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05394_sleep_%'
ORDER BY log_comment;

-- When reordering is allowed, the cheap argument is still executed first in a lazily executed `and`.
SELECT sum(if(number % 3 = 0, intDiv(10, number) > 1 AND number > 0, 0)), sum(number > 0)
FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;
SELECT sum(if(number % 3 = 0, intDiv(10, number) > 1 AND number > 0, 0)), sum(number > 0)
FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0; -- { serverError ILLEGAL_DIVISION }

-- An unsigned divisor of a signed dividend is cast to a signed type, so `intDiv(Int8(-128), UInt8(255))` divides by
-- minus one and throws. Such a condition is not moved in front of the condition that guards it in the AND chain of `WHERE`.
SELECT count()
FROM values('x Int8', (-128), (1))
WHERE repeat(toString(x), 3) != '-128-128-128' AND intDiv(x, toUInt8(255)) != 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    query_plan_merge_filters = 1;
SELECT count()
FROM values('x Int8', (-128), (1))
WHERE repeat(toString(x), 3) != '-128-128-128' AND modulo(x, toUInt8(255)) != 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    query_plan_merge_filters = 1;
-- A divisor that stays positive after the cast is safe.
SELECT count()
FROM values('x Int8', (-128), (1))
WHERE repeat(toString(x), 3) != '-128-128-128' AND intDiv(x, toUInt8(127)) = 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, compile_expressions = 0,
    query_plan_merge_filters = 1;
