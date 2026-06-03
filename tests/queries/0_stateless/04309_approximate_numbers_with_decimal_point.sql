-- Whole-number floats gain a trailing decimal point only when the setting is enabled; integers are never affected.
SELECT 1.0, 1., 1 SETTINGS output_format_approximate_numbers_with_decimal_point=0;
SELECT 1.0, 1., 1 SETTINGS output_format_approximate_numbers_with_decimal_point=1;

SELECT number::Float32 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=0;
SELECT number::Float32 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=1;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=0;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=1;

-- Floats that already contain a fractional part are unchanged.
SELECT 1.5::Float64, 0.25::Float32 SETTINGS output_format_approximate_numbers_with_decimal_point=1;

-- Special values must not get a spurious decimal point.
SELECT (1/0)::Float64, (-1/0)::Float64, (0/0)::Float64 SETTINGS output_format_approximate_numbers_with_decimal_point=1;

-- Large integer-valued floats taking the rounding fast path also get a decimal point.
SELECT 1e18::Float64, 1e9::Float32 SETTINGS output_format_approximate_numbers_with_decimal_point=1;

-- Decimal values follow the same rule: whole numbers gain a trailing decimal point, fractional values are unchanged.
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=0;
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_approximate_numbers_with_decimal_point=1;
SELECT 1.50::Decimal(10, 2), 1::Decimal(10, 2) SETTINGS output_format_approximate_numbers_with_decimal_point=1;
