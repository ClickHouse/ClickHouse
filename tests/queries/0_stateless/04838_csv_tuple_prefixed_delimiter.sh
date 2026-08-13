#!/usr/bin/env bash
# shellcheck disable=SC2086
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROW_FORMAT_DIR="$CUR_DIR/$CLICKHOUSE_DATABASE"
ROW_FORMAT_PATH="$ROW_FORMAT_DIR/04838_csv_tuple_prefixed_delimiter.template"

mkdir -p "$ROW_FORMAT_DIR"
trap 'rm -f "$ROW_FORMAT_PATH"' EXIT
printf '${t:CSV}:|${n:CSV}\n' > "$ROW_FORMAT_PATH"

echo 'Template'
$CLICKHOUSE_LOCAL --query="
    SELECT
        tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8) AS t,
        42::UInt8 AS n
    FORMAT Template
    SETTINGS
        output_format_csv_quote_date_time_types = 0,
        format_csv_delimiter = ':',
        format_template_row_format = '\${t:CSV}:|\${n:CSV}\n',
        format_template_resultset_format = '\${data}'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(DateTime('UTC'), UInt8), n UInt8" \
        --input-format Template \
        --format_template_row "$ROW_FORMAT_PATH" \
        --format_csv_delimiter ':' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated'
$CLICKHOUSE_LOCAL --query="
    SELECT
        tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1::UInt8) AS t,
        42::UInt8 AS n
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_quote_date_time_types = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':|'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(DateTime('UTC'), UInt8), n UInt8" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated replays a failed flattened tuple parse'
$CLICKHOUSE_LOCAL --query="
    SELECT
        tuple(
            'value'::String,
            toDateTime('2024-01-17 08:30:00', 'UTC'),
            1::UInt8) AS t,
        42::UInt8 AS n
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':|'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(String, DateTime('UTC'), UInt8), n UInt8" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated preserves an ambiguous flattened tuple boundary'
$CLICKHOUSE_LOCAL --query="
    SELECT
        tuple(
            '(''y'',-5,''2024-01-17 08:30:00'')'::String,
            -5::Int8,
            toDateTime64('2024-01-17 08:30:00', 3, 'UTC')) AS t,
        7::UInt8 AS n
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_quote_date_time_types = 0,
        date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1,
        format_csv_delimiter = '.',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = '.-'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(String, Int8, DateTime64(3, 'UTC')), n UInt8" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands 1 \
        --format_csv_delimiter '.' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter '.-' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated last tuple uses the row delimiter'
$CLICKHOUSE_LOCAL --query="
    SELECT
        toUInt8(number + 42) AS n,
        tuple(toUInt8(number + 1), toDateTime('2024-01-17 08:30:00', 'UTC')) AS t
    FROM numbers(2)
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = '|'" |
    $CLICKHOUSE_LOCAL \
        --structure "n UInt8, t Tuple(UInt8, DateTime('UTC'))" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter '|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated last flattened tuple keeps a quoted first element'
$CLICKHOUSE_LOCAL --query="
    SELECT
        toUInt8(number + 42) AS n,
        tuple(concat('value', toString(number)), toDateTime('2024-01-17 08:30:00', 'UTC')) AS t
    FROM numbers(2)
    FORMAT CustomSeparated
    SETTINGS
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = '|'" |
    $CLICKHOUSE_LOCAL \
        --structure "n UInt8, t Tuple(String, DateTime('UTC'))" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter '|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated whole string tuple keeps prefixed row boundaries'
$CLICKHOUSE_LOCAL --query="
    SELECT tuple(concat('left', toString(number)), concat('right', toString(number))) AS t
    FROM numbers(2)
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ',',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = ',|'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(String, String)" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ',' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter ',|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated last whole tuple uses a prefixed row delimiter'
$CLICKHOUSE_LOCAL --query="
    SELECT
        toUInt8(number + 42) AS n,
        tuple(toUInt8(number + 1), toDateTime('2024-01-17 08:30:00', 'UTC')) AS t
    FROM numbers(2)
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = ':|'" |
    $CLICKHOUSE_LOCAL \
        --structure "n UInt8, t Tuple(UInt8, DateTime('UTC'))" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter ':|' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated last whole tuple may end at EOF'
$CLICKHOUSE_LOCAL --query="
    SELECT tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')) AS t
    FORMAT CustomSeparated
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = ''" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(UInt8, DateTime('UTC'))" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter '' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'Template whole tuple supports an empty following delimiter'
printf '${t:CSV}${n:CSV}\n' > "$ROW_FORMAT_PATH"
$CLICKHOUSE_LOCAL --query="
    SELECT
        tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')) AS t,
        42::UInt8 AS n
    FORMAT Template
    SETTINGS
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_template_row_format = '\${t:CSV}\${n:CSV}\n',
        format_template_resultset_format = '\${data}'" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(UInt8, DateTime('UTC')), n UInt8" \
        --input-format Template \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --format_template_row "$ROW_FORMAT_PATH" \
        --format_csv_delimiter ':' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'CustomSeparated accepts an unquoted whole tuple'
printf "('value',5)|42\n" |
    $CLICKHOUSE_LOCAL \
        --structure "t Tuple(String, UInt8), n UInt8" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter '|' \
        --format_custom_row_after_delimiter $'\n' \
        --query 'SELECT * FROM table FORMAT TSVRaw'

echo 'Variant preserves a whole tuple through a string buffer'
$CLICKHOUSE_LOCAL --query="
    SELECT CAST(
        tuple(1::UInt8, toDateTime('2024-01-17 08:30:00', 'UTC')),
        'Variant(Tuple(UInt8, DateTime(\\'UTC\\')), String)') AS v
    FORMAT CustomSeparated
    SETTINGS
        allow_experimental_variant_type = 1,
        output_format_csv_serialize_tuple_into_separate_columns = 0,
        format_csv_delimiter = ':',
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = ':',
        format_custom_row_after_delimiter = '|'" |
    $CLICKHOUSE_LOCAL \
        --structure "v Variant(Tuple(UInt8, DateTime('UTC')), String)" \
        --input-format CustomSeparated \
        --input_format_custom_detect_header 0 \
        --input_format_parallel_parsing 0 \
        --max_read_buffer_size 1 \
        --allow_experimental_variant_type 1 \
        --format_csv_delimiter ':' \
        --format_custom_escaping_rule CSV \
        --format_custom_field_delimiter ':' \
        --format_custom_row_after_delimiter '|' \
        --query 'SELECT variantType(v), v FROM table FORMAT TSVRaw'

echo 'Variant forwards row-specific tuple quoting'
variant_output=$($CLICKHOUSE_LOCAL --query "
    SELECT tuple(
        arrayJoin([
            CAST(tuple(toDateTime('2024-01-17 08:30:00', 'UTC'), 1), 'Variant(Tuple(DateTime(''UTC''), UInt8), UInt8)'),
            CAST(7, 'Variant(Tuple(DateTime(''UTC''), UInt8), UInt8)'),
            CAST(NULL, 'Variant(Tuple(DateTime(''UTC''), UInt8), UInt8)')
        ]),
        2::UInt8
    ) AS value, 42::UInt8 AS suffix
    FORMAT CustomSeparated
    SETTINGS
        allow_experimental_variant_type = 1,
        format_csv_delimiter = ':',
        output_format_csv_quote_date_time_types = 0,
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = '|',
        format_custom_row_after_delimiter = '\n'
")
printf '%s\n' "$variant_output"
printf '%s\n' "$variant_output" | $CLICKHOUSE_LOCAL \
    --structure "value Tuple(Variant(Tuple(DateTime('UTC'), UInt8), UInt8), UInt8), suffix UInt8" \
    --input-format CustomSeparated \
    --input_format_custom_detect_header 0 \
    --input_format_parallel_parsing 0 \
    --allow_experimental_variant_type 1 \
    --format_csv_delimiter ':' \
    --format_custom_escaping_rule CSV \
    --format_custom_field_delimiter '|' \
    --format_custom_row_after_delimiter $'\n' \
    --query 'SELECT variantType(value.1), value, suffix FROM table FORMAT TSVRaw'

echo 'Dynamic forwards row-specific tuple quoting'
dynamic_output=$($CLICKHOUSE_LOCAL --query "
    SELECT tuple(
        arrayJoin([
            CAST(tuple(toDateTime('2024-01-17 08:30:00'), 1::Int64), 'Dynamic'),
            CAST(7, 'Dynamic'),
            CAST(NULL, 'Dynamic')
        ]),
        2::UInt8
    ) AS value, 42::UInt8 AS suffix
    FORMAT CustomSeparated
    SETTINGS
        allow_experimental_dynamic_type = 1,
        format_csv_delimiter = ':',
        output_format_csv_quote_date_time_types = 0,
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = '|',
        format_custom_row_after_delimiter = '\n'
")
printf '%s\n' "$dynamic_output"
printf '%s\n' "$dynamic_output" | $CLICKHOUSE_LOCAL \
    --structure "value Tuple(Dynamic, UInt8), suffix UInt8" \
    --input-format CustomSeparated \
    --input_format_custom_detect_header 0 \
    --input_format_parallel_parsing 0 \
    --allow_experimental_dynamic_type 1 \
    --format_csv_delimiter ':' \
    --format_custom_escaping_rule CSV \
    --format_custom_field_delimiter '|' \
    --format_custom_row_after_delimiter $'\n' \
    --query 'SELECT dynamicType(value.1), value, suffix FROM table FORMAT TSVRaw'

echo 'Shared Dynamic forwards row-specific tuple quoting'
shared_dynamic_output=$($CLICKHOUSE_LOCAL --query "
    SELECT tuple(
        CAST(tuple(toDateTime('2024-01-17 08:30:00'), 1::Int64), 'Dynamic(max_types=0)'),
        2::UInt8
    ) AS value, 42::UInt8 AS suffix
    FORMAT CustomSeparated
    SETTINGS
        allow_experimental_dynamic_type = 1,
        format_csv_delimiter = ':',
        output_format_csv_quote_date_time_types = 0,
        format_custom_escaping_rule = 'CSV',
        format_custom_field_delimiter = '|',
        format_custom_row_after_delimiter = '\n'
")
printf '%s\n' "$shared_dynamic_output"
printf '%s\n' "$shared_dynamic_output" | $CLICKHOUSE_LOCAL \
    --structure "value Tuple(Dynamic(max_types=0), UInt8), suffix UInt8" \
    --input-format CustomSeparated \
    --input_format_custom_detect_header 0 \
    --input_format_parallel_parsing 0 \
    --allow_experimental_dynamic_type 1 \
    --format_csv_delimiter ':' \
    --format_custom_escaping_rule CSV \
    --format_custom_field_delimiter '|' \
    --format_custom_row_after_delimiter $'\n' \
    --query 'SELECT dynamicType(value.1), value, suffix FROM table FORMAT TSVRaw'
