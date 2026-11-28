-- Deserializing a garbage aggregate state whose vector size field is huge threw a bare Poco::Exception
-- ("Too large vector size.", reported as POCO_EXCEPTION, code 1000) from readBinary(std::vector). It is a
-- proper TOO_LARGE_ARRAY_SIZE now. Found by json_ast_sql_execution_fuzzer.
SELECT arrayFirst(CAST(unhex('AggregateFunction(stochasticLinearRegression(0.1, 0, 1, \'Momentum\'), Float64, Float64)'), 'AggregateFunction(stochasticLinearRegression(0.1, 0, 1, \'Momentum\'), Float64, Float64)')); -- { serverError TOO_LARGE_ARRAY_SIZE }
