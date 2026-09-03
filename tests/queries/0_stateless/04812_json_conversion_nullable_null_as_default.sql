-- Optimized JSON-to-JSON conversion: a typed path changing from Nullable(T) to T must
-- respect input_format_null_as_default. With it enabled a source null becomes the default;
-- with it disabled the conversion must raise, matching the format+parse path.

-- null_as_default = 1 (default): null becomes the default value on both paths.
SELECT ('{"a":null}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":null}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS json_use_optimized_type_conversion = 1;

-- Non-null values convert normally regardless of the setting.
SELECT ('{"a":5}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS json_use_optimized_type_conversion = 0;
SELECT ('{"a":5}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS json_use_optimized_type_conversion = 1;

-- null_as_default = 0: a source null must raise on both paths.
SELECT ('{"a":null}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS input_format_null_as_default = 0, json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT ('{"a":null}'::JSON(a Nullable(UInt8))::JSON(a UInt8)).a SETTINGS input_format_null_as_default = 0, json_use_optimized_type_conversion = 1; -- { serverError INCORRECT_DATA }
