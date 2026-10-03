-- Tests for the setting `short_circuit_function_evaluation_reorder_arguments`.

-- The first argument is heavy (it contains `intDiv`, which is executed lazily), the second one is cheap.
-- With reordering, the cheap argument is executed first, and `intDiv` is executed only on the rows where it is true.
SELECT sum(intDiv(10, number) > 1 AND number > 0) FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;
SELECT sum(intDiv(10, number) = 0 OR number = 0) FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;
SELECT sum(intDiv(10, number) > 1 AND number > 0) FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0; -- { serverError ILLEGAL_DIVISION }

-- The same in `WHERE`, where the AND chain is evaluated by consecutive filters. `numbers` would use `number > 0`
-- as a range, so the condition is written differently.
SELECT count() FROM numbers(100) WHERE intDiv(10, number % 10) > 1 AND number % 10 != 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;
SELECT count() FROM numbers(100) WHERE intDiv(10, number % 10) > 1 AND number % 10 != 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0; -- { serverError ILLEGAL_DIVISION }
-- A condition that can throw is not moved to the front of the chain.
SELECT count() FROM numbers(100) WHERE number % 10 != 0 AND intDiv(10, number % 10) > 1 AND number % 3 = 0
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;

-- The same inside another short-circuit function, where the `and` itself is executed lazily.
SELECT sum(if(number < 5, intDiv(10, number) > 1 AND number > 0, 0)) FROM numbers(10)
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;

-- An argument that can throw is never executed before the arguments that precede it,
-- no matter what the statistics say: `x != 0` always guards `intDiv(1000, x)`.
SELECT sum(x != 0 AND intDiv(1000, x) > 1 AND x % 7 = 0) FROM (SELECT number % 100 AS x FROM numbers(100000))
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 1000;
SELECT sum(x != 0 AND intDiv(1000, x) > 1 AND x % 7 = 0) FROM (SELECT number % 100 AS x FROM numbers(100000))
SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 1000;
SELECT sum(x = 0 OR intDiv(1000, x) > 1 OR x % 7 = 0) FROM (SELECT number % 100 AS x FROM numbers(100000))
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 1000;
SELECT count() FROM (SELECT if(number % 2, 'Decimal', 'String') AS t, if(number % 2, '10', 'abc') AS s FROM numbers(100000))
WHERE t = 'Decimal' AND toInt8(s) <= 65 AND length(s) > 0
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 1000;

-- The result does not depend on the order, including the ternary logic with NULL.
SELECT
    sum(cityHash64(number < 10000000 AND if(number % 3 = 0, NULL, number % 5 = 0) AND number % 7 != 0)),
    sum(cityHash64(number > 10000000 OR if(number % 3 = 0, NULL, number % 5 = 0) OR number % 7 != 0))
FROM numbers(100000)
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 1000;
SELECT
    sum(cityHash64(number < 10000000 AND if(number % 3 = 0, NULL, number % 5 = 0) AND number % 7 != 0)),
    sum(cityHash64(number > 10000000 OR if(number % 3 = 0, NULL, number % 5 = 0) OR number % 7 != 0))
FROM numbers(100000)
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 0, max_block_size = 1000;

-- The second argument decides almost no rows, the third one decides almost all of them,
-- so after the first blocks the third argument is executed before the second one.
SELECT sum(number < 10000000 AND number % 1000 != 5 AND bitAnd(number, 1023) = 7) FROM numbers(1000000)
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 1, max_block_size = 8192,
    log_comment = '05259_reorder_1';
SELECT sum(number < 10000000 AND number % 1000 != 5 AND bitAnd(number, 1023) = 7) FROM numbers(1000000)
SETTINGS short_circuit_function_evaluation = 'force_enable', short_circuit_function_evaluation_reorder_arguments = 0, max_block_size = 8192,
    log_comment = '05259_reorder_0';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ShortCircuitArgumentsReordered'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05259_reorder_%'
ORDER BY log_comment;
