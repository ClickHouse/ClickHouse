-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- `CompiledFunctionRows` counts rows once per function fused into a compiled expression,
-- `InterpretedFunctionRows` once per function that runs without JIT compilation.

SET log_queries = 1;

-- Three fused functions (`plus`, `multiply`, `plus`) over 1000 rows.
SELECT number + number * 2 + 1 FROM numbers(1000) FORMAT Null
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_rows_1_compiled';

-- The same expression interpreted.
SELECT number + number * 2 + 1 FROM numbers(1000) FORMAT Null
SETTINGS compile_expressions = 0, log_comment = 'jit_rows_2_interpreted';

-- `toString` cannot be compiled and runs on the result of the compiled expression.
SELECT toString(number + number * 2 + 1) FROM numbers(1000) FORMAT Null
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_rows_3_mixed';

-- The branch of `if` is executed lazily, only for the 300 rows where the condition holds.
SELECT if(number < 300, toString(number), '') FROM numbers(1000) FORMAT Null
SETTINGS compile_expressions = 0, short_circuit_function_evaluation = 'enable', log_comment = 'jit_rows_4_short_circuit';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['CompiledFunctionRows'], ProfileEvents['InterpretedFunctionRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'jit_rows_%' AND event_date >= yesterday()
ORDER BY log_comment;
