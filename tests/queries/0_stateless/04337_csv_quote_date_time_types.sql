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

SELECT 'negative subsecond DateTime64 Unix timestamp is unquoted with safe delimiter' FORMAT TSVRaw;
SELECT formatRow('CSV', toDateTime64(-0.456, 3, 'UTC')) = '-0.456\n'
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    date_time_output_format = 'unix_timestamp',
    format_csv_delimiter = '|';

SELECT 'negative subsecond DateTime64 Unix timestamp is quoted with conflicting delimiter' FORMAT TSVRaw;
SELECT formatRow('CSV', toDateTime64(-0.456, 3, 'UTC')) = '"-0.456"\n'
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    date_time_output_format = 'unix_timestamp',
    format_csv_delimiter = '-';

SELECT 'DateTime64 dot quoting follows trimmed fractional output' FORMAT TSVRaw;
SELECT
    startsWith(formatRow('CSV', toDateTime64('2024-01-18 09:45:01', 3, 'UTC')), '"'),
    startsWith(formatRow('CSV', toDateTime64('2024-01-18 09:45:01.123', 3, 'UTC')), '"')
FORMAT TSVRaw
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = '.',
    date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1;

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

SELECT 'CustomSeparated CSV escaping keeps a boundary without a field delimiter' FORMAT TSVRaw;
SELECT toTime64('12:30:00.456', 3) AS t, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template CSV escaping uses its following delimiter' FORMAT TSVRaw;
SELECT toDate('2024-01-15') AS d, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_template_row_format = '${d:CSV}-${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'Template CSV escaping keeps a boundary without a delimiter' FORMAT TSVRaw;
SELECT toDateTime(1234567890, 'UTC') AS dt, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    date_time_output_format = 'unix_timestamp',
    format_template_row_format = '${dt:CSV}${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated keeps a divergent tuple delimiter in one CSV field' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(DateTime(\'UTC\'), DateTime(\'UTC\')), n UInt8',
    '"(\'2024-01-17 08:30:00\',\'2024-01-18 09:45:01\')"|42\n')
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'CustomSeparated temporal tuple fallback is auto-detected on input' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(DateTime(\'UTC\'), DateTime(\'UTC\')), n UInt8',
    '"(\'2024-01-17 08:30:00\',\'2024-01-18 09:45:01\')"|42\n')
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'CustomSeparated flattened tuple is not mistaken for whole tuple input' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(String, DateTime(\'UTC\')), n UInt8',
    '"(value)":"2024-01-18 09:45:01"|42\n')
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template keeps a divergent tuple delimiter in one CSV field' FORMAT TSVRaw;
SELECT
    tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), toDateTime('2024-01-18 09:45:01', 'UTC')) AS t,
    42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'Template keeps non-temporal tuple columns separate' FORMAT TSVRaw;
SELECT tuple(1::UInt8, 2::UInt8) AS t, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated last temporal tuple element uses outer delimiter' FORMAT TSVRaw;
SELECT tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')) AS t, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'CustomSeparated inverse delimiters round-trip a whole temporal tuple' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(UInt8, DateTime(\'UTC\')), n UInt8',
    (
        SELECT formatRow(
            'CustomSeparated',
            tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')),
            42::UInt8)
    ))
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = '|',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = ':',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template last temporal tuple element uses outer delimiter' FORMAT TSVRaw;
SELECT tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')) AS t, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated preserves a leading nested tuple boundary' FORMAT TSVRaw;
SELECT tuple(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 2::UInt8) AS t, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template preserves a leading nested tuple boundary' FORMAT TSVRaw;
SELECT tuple(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 2::UInt8) AS t, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated trailing nested tuple fallback uses the whole field' FORMAT TSVRaw;
SELECT tuple(1::UInt8, tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 2::UInt8)) AS t, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template trailing nested tuple fallback uses the whole field' FORMAT TSVRaw;
SELECT tuple(1::UInt8, tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 2::UInt8)) AS t, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated keeps a safe trailing nested tuple flattened' FORMAT TSVRaw;
SELECT tuple(1::UInt8, tuple(2::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC'))) AS t, 42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template keeps a safe trailing nested tuple flattened' FORMAT TSVRaw;
SELECT tuple(1::UInt8, tuple(2::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC'))) AS t, 42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated trailing nested tuple fallback round-trips' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(UInt8, Tuple(DateTime(\'UTC\'), UInt8)), n UInt8',
    (
        SELECT formatRow(
            'CustomSeparated',
            tuple(1::UInt8, tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 2::UInt8)),
            42::UInt8)
    ))
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SET enable_nullable_tuple_type = 1;

SELECT 'CustomSeparated keeps Nullable Tuple as one flattened element' FORMAT TSVRaw;
SELECT
    tuple(
        CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 'Nullable(Tuple(DateTime(\'UTC\'), UInt8))'),
        2::UInt8) AS t,
    42 AS n
FORMAT CustomSeparated
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template keeps Nullable Tuple as one flattened element' FORMAT TSVRaw;
SELECT
    tuple(
        CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 'Nullable(Tuple(DateTime(\'UTC\'), UInt8))'),
        2::UInt8) AS t,
    42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated Nullable Tuple element round-trips' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(Nullable(Tuple(DateTime(\'UTC\'), UInt8)), UInt8), n UInt8',
    (
        SELECT formatRow(
            'CustomSeparated',
            tuple(
                CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 'Nullable(Tuple(DateTime(\'UTC\'), UInt8))'),
                2::UInt8),
            42::UInt8)
    ))
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template final Nullable Tuple remains flattened' FORMAT TSVRaw;
SELECT
    tuple(
        2::UInt8,
        CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 'Nullable(Tuple(DateTime(\'UTC\'), UInt8))')) AS t,
    42 AS n
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_template_row_format = '${t:CSV}|${n:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'CustomSeparated nested Nullable Tuple element round-trips' FORMAT TSVRaw;
SELECT *
FROM format(
    CustomSeparated,
    't Tuple(Tuple(Nullable(Tuple(DateTime(\'UTC\'), UInt8)), UInt8), UInt8), n UInt8',
    (
        SELECT formatRow(
            'CustomSeparated',
            tuple(
                tuple(
                    CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8), 'Nullable(Tuple(DateTime(\'UTC\'), UInt8))'),
                    2::UInt8),
                3::UInt8),
            42::UInt8)
    ))
FORMAT TSVRaw
SETTINGS
    input_format_custom_detect_header = 0,
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = ':',
    format_custom_escaping_rule = 'CSV',
    format_custom_field_delimiter = '|',
    format_custom_row_after_delimiter = '\n';

SELECT 'Template DateTime64 tuple follows trimmed whole-second output' FORMAT TSVRaw;
SELECT tuple(toDateTime64('2024-01-18 09:45:01', 3, 'UTC'), 42::UInt8) AS t
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = '.',
    date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
    format_template_row_format = '${t:CSV}\n',
    format_template_resultset_format = '${data}';

SELECT 'Template DateTime64 tuple keeps fractional delimiter safe' FORMAT TSVRaw;
SELECT tuple(toDateTime64('2024-01-18 09:45:01.123', 3, 'UTC'), 42::UInt8) AS t
FORMAT Template
SETTINGS
    output_format_csv_quote_date_time_types = 0,
    format_csv_delimiter = '.',
    date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
    format_template_row_format = '${t:CSV}\n',
    format_template_resultset_format = '${data}';
