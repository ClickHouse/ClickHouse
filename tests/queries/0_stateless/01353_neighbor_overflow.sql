SET allow_deprecated_error_prone_window_functions = 1;
SELECT neighbor(toString(number), -9223372036854775808) FROM numbers(100); -- { serverError ARGUMENT_OUT_OF_BOUND }
WITH neighbor(toString(number), toInt64(rand64())) AS x SELECT * FROM system.numbers WHERE NOT ignore(x); -- { serverError ARGUMENT_OUT_OF_BOUND }
