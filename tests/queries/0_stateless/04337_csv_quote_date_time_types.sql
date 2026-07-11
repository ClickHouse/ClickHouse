SET enable_time_time64_type = 1;

SELECT 'default CSV output quotes date and time types' FORMAT TSVRaw;
SELECT *
FROM format(
    CSV,
    'd Date, d32 Date32, dt DateTime(\'UTC\'), dt64 DateTime64(3, \'UTC\'), t Time, t64 Time64(3), n UInt8, s String',
    '2024-01-15,2024-01-16,2024-01-17 08:30:00,2024-01-18 09:45:01.123,12:30:00,12:30:00.456,42,value')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 1;

SELECT 'disabled date/time CSV quoting keeps strings quoted' FORMAT TSVRaw;
SELECT *
FROM format(
    CSV,
    'd Date, d32 Date32, dt DateTime(\'UTC\'), dt64 DateTime64(3, \'UTC\'), t Time, t64 Time64(3), n UInt8, s String',
    '2024-01-15,2024-01-16,2024-01-17 08:30:00,2024-01-18 09:45:01.123,12:30:00,12:30:00.456,42,value')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0;

SELECT 'nullable values with disabled date/time CSV quoting' FORMAT TSVRaw;
SELECT *
FROM format(
    CSV,
    'd Nullable(Date), dt Nullable(DateTime(\'UTC\')), s Nullable(String)',
    '2024-01-15,2024-01-17 08:30:00,value\n\\N,\\N,\\N')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0;

SELECT 'low cardinality date with disabled date/time CSV quoting' FORMAT TSVRaw;
SELECT toLowCardinality(toDate('2024-01-15')) AS d FORMAT CSV SETTINGS output_format_csv_quote_date_time_types = 0;

SELECT 'CSVWithNames with disabled date/time CSV quoting' FORMAT TSVRaw;
SELECT toDate('2024-01-15') AS d, 'value' AS s FORMAT CSVWithNames SETTINGS output_format_csv_quote_date_time_types = 0;

SELECT 'Time remains quoted with conflicting colon CSV delimiter' FORMAT TSVRaw;
SELECT *
FROM format(CSV, 't Time, n UInt8', '"12:30:00":42')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0, format_csv_delimiter = ':';

SELECT 'Time64 remains quoted with conflicting colon CSV delimiter' FORMAT TSVRaw;
SELECT *
FROM format(CSV, 't Time64(3), n UInt8', '"12:30:00.456":42')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0, format_csv_delimiter = ':';

SELECT 'Nullable Time remains quoted with conflicting colon CSV delimiter' FORMAT TSVRaw;
SELECT *
FROM format(CSV, 't Nullable(Time), n UInt8', '"12:30:00":42')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0, format_csv_delimiter = ':', input_format_csv_use_default_on_bad_values = 1;

SELECT 'Nullable Time64 remains quoted with conflicting colon CSV delimiter' FORMAT TSVRaw;
SELECT *
FROM format(CSV, 't Nullable(Time64(3)), n UInt8', '"12:30:00.456":42')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0, format_csv_delimiter = ':', input_format_csv_use_default_on_bad_values = 1;

SELECT 'temporal values remain quoted with numeric CSV delimiter' FORMAT TSVRaw;
SELECT *
FROM format(
    CSV,
    'd Date, d32 Date32, dt DateTime(\'UTC\'), dt64 DateTime64(3, \'UTC\'), t Time, t64 Time64(3), n UInt8',
    '"2024-01-15"1"2024-01-16"1"1234567890"1"1234567890.123"1"12:30:00"1"12:30:00.456"142')
FORMAT CSV
SETTINGS output_format_csv_quote_date_time_types = 0, format_csv_delimiter = '1', date_time_output_format = 'unix_timestamp';

SELECT 'negative subsecond DateTime64 Unix timestamp remains quoted' FORMAT TSVRaw;
SELECT startsWith(formatRow('CSV', toDateTime64(-0.456, 3, 'UTC')), '"')
SETTINGS output_format_csv_quote_date_time_types = 0, date_time_output_format = 'unix_timestamp';

SELECT 'CustomSeparated CSV escaping uses its field delimiter' FORMAT TSVRaw;
SELECT toDate('2024-01-15') AS d, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '-',
    format_custom_row_after_delimiter = '\n';

SELECT 'CustomSeparated CSV escaping uses its final row delimiter' FORMAT TSVRaw;
SELECT toDate('2024-01-15') AS d
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '-',
    format_custom_result_after_delimiter = '\n';

SELECT 'Template CSV escaping uses its following delimiter' FORMAT TSVRaw;
SELECT toDate('2024-01-15') AS d, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_template_row_format = '${d:CSV}-${n:CSV}\n',
    format_template_resultset_format = '${data}';
