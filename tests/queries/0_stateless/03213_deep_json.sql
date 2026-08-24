-- The default limit works.
SELECT * FROM format("JSONCompactEachRow", 'x UInt32, y UInt32', REPEAT('[1,1,', 100000)) SETTINGS input_format_json_compact_allow_variable_number_of_columns = 1; -- { serverError TOO_DEEP_RECURSION, INCORRECT_DATA }
-- Even if we relax the limit, it is also safe.
SET input_format_json_max_depth = 100000;
SELECT * FROM format("JSONCompactEachRow", 'x UInt32, y UInt32', REPEAT('[1,1,', 100000)) SETTINGS input_format_json_compact_allow_variable_number_of_columns = 1; -- { serverError TOO_DEEP_RECURSION, INCORRECT_DATA }
