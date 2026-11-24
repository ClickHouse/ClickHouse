-- Optimized JSON-to-JSON conversion: when a changed typed path falls back to format+parse
-- (here String -> UInt64 with numeric-from-string inference), bad rows must only be defaulted
-- when type_json_skip_invalid_typed_paths is enabled; otherwise the conversion must raise,
-- matching the format+parse path.

SET input_format_json_try_infer_numbers_from_strings = 1;

-- type_json_skip_invalid_typed_paths = 1: the bad row becomes the default on both paths.
SELECT arrayJoin(['{"a":"123"}', '{"a":"bad"}'])::JSON(a String)::JSON(a UInt64) SETTINGS type_json_skip_invalid_typed_paths = 1, json_use_optimized_type_conversion = 0;
SELECT arrayJoin(['{"a":"123"}', '{"a":"bad"}'])::JSON(a String)::JSON(a UInt64) SETTINGS type_json_skip_invalid_typed_paths = 1, json_use_optimized_type_conversion = 1;

-- type_json_skip_invalid_typed_paths = 0 (default): the bad row must raise on both paths.
SELECT arrayJoin(['{"a":"123"}', '{"a":"bad"}'])::JSON(a String)::JSON(a UInt64) SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT arrayJoin(['{"a":"123"}', '{"a":"bad"}'])::JSON(a String)::JSON(a UInt64) SETTINGS json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }
