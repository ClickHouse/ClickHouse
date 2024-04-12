SELECT 1.0, 1., 1 SETTINGS output_format_approximate_numbers_with_period=0;
SELECT 1.0, 1., 1 SETTINGS output_format_approximate_numbers_with_period=1;
SELECT number::Float32 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=0;
SELECT number::Float32 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=1;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=0;
SELECT number::Float64 FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=1;
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=0;
SELECT number::Decimal(2) FROM numbers(2) SETTINGS output_format_approximate_numbers_with_period=1;
