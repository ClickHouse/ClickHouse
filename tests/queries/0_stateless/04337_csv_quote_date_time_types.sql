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
