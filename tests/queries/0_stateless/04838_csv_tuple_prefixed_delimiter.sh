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
