-- Whole-number floats gain a trailing decimal point only when the setting is enabled; integers are never affected.
SELECT 1.0, 1., 1 SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=0;
SELECT 1.0, 1., 1 SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

SELECT number::Float32 FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=0;
SELECT number::Float32 FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=0;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- Floats that already contain a fractional part are unchanged.
SELECT 1.5::Float64, 0.25::Float32 SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- Special values must not get a spurious decimal point.
SELECT (1/0)::Float64, (-1/0)::Float64, (0/0)::Float64 SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- Large integer-valued floats taking the rounding fast path also get a decimal point.
SELECT 1e18::Float64, 1e9::Float32 SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- Large integer-valued floats outside the itoa fast-path ranges (printed via the shortest-representation
-- path as a bare integer, without an exponent) must also get a trailing decimal point.
SELECT toFloat32(1.23e20), toFloat64(1e20) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- Decimal values follow the same rule: whole numbers gain a trailing decimal point, fractional values are unchanged.
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=0;
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;
SELECT 1.50::Decimal(10, 2), 1::Decimal(10, 2) SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;

-- JSON output must stay valid: the forced decimal point is not applied to numbers in JSON, otherwise a bare `1.`
-- would not be a valid JSON number with the default output_format_json_quote_decimals=0.
SELECT 1::Decimal(10, 2) AS d, 1::Float64 AS f FORMAT JSONEachRow SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1;
SELECT 1::Decimal(10, 2) AS d FORMAT JSONEachRow SETTINGS output_format_always_write_decimal_point_in_float_and_decimal=1, output_format_json_quote_decimals=1;
