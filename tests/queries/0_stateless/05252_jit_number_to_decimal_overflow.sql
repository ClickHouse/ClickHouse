-- A JIT-compiled number -> Decimal conversion must raise DECIMAL_OVERFLOW like the interpreter, not silently wrap.
-- `convertCompileImpl` truncated the source to the Decimal's native width and multiplied by the scale with no range
-- check, so toDecimal32(toInt128(87960930222084), 1) returned 4 with compiled expressions on.
-- Found by the json_ast_sql_execution_fuzzer differential oracle (default vs compile_expressions=0).
SELECT toDecimal32(toInt128(number), 1) FROM numbers(87960930222084, 1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(toInt128(number), 1) FROM numbers(87960930222084, 1) SETTINGS compile_expressions = 0; -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(toInt128(number), 10) FROM numbers(87960930222084, 1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0; -- { serverError DECIMAL_OVERFLOW }
-- Valid conversions still work with compiled expressions.
SELECT toDecimal32(toInt128(number), 1) FROM numbers(5, 1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
SELECT toDecimal128(toInt64(number), 6) FROM numbers(5, 1) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
