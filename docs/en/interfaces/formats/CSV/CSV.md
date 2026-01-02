---
alias: []
description: 'Documentation for the CSV format'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
---

## Description {#description}

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).
When formatting, rows are enclosed in double quotes. A double quote inside a string is output as two double quotes in a row. 
There are no other rules for escaping characters. 

- Date and date-time are enclosed in double quotes. 
- Numbers are output without quotes.
- Values are separated by a delimiter character, which is `,` by default. The delimiter character is defined in the setting [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter). 
- Rows are separated using the Unix line feed (LF). 
- Arrays are serialized in CSV as follows: 
  - first, the array is serialized to a string as in TabSeparated format
  - The resulting string is output to CSV in double quotes.
- Tuples in CSV format are serialized as separate columns (that is, their nesting in the tuple is lost).

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
By default, the delimiter is `,` 
See the [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter) setting for more information.
:::

When parsing, all values can be parsed either with or without quotes. Both double and single quotes are supported.

Rows can also be arranged without quotes. In this case, they are parsed up to the delimiter character or line feed (CR or LF).
However, in violation of the RFC, when parsing rows without quotes, the leading and trailing spaces and tabs are ignored.
The line feed supports: Unix (LF), Windows (CR LF) and Mac OS Classic (CR LF) types.

`NULL` is formatted according to setting [format_csv_null_representation](/operations/settings/settings-formats.md/#format_csv_null_representation) (the default value is `\N`).

In the input data, `ENUM` values can be represented as names or as ids. 
First, we try to match the input value to the ENUM name. 
If we fail and the input value is a number, we try to match this number to the ENUM id.
If input data contains only ENUM ids, it's recommended to enable the setting [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) to optimize `ENUM` parsing.

## Example Usage {#example-usage}

## Format Settings {#format-settings}

| Setting                                                                                                                                                            | Description                                                                                                        | Default | Notes                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [format_csv_delimiter](/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                     | the character to be considered as a delimiter in CSV data.                                                         | `,`     |                                                                                                                                                                                              |
| [format_csv_allow_single_quotes](/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                 | allow strings in single quotes.                                                                                    | `true`  |                                                                                                                                                                                              |
| [format_csv_allow_double_quotes](/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                 | allow strings in double quotes.                                                                                    | `true`  |                                                                                                                                                                                              | 
| [format_csv_null_representation](/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                 | custom NULL representation in CSV format.                                                                          | `\N`    |                                                                                                                                                                                              |   
| [input_format_csv_empty_as_default](/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                           | treat empty fields in CSV input as default values.                                                                 | `true`  | For complex default expressions, [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. | 
| [input_format_csv_enum_as_number](/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                               | treat inserted enum values in CSV formats as enum indices.                                                         | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_best_effort_in_schema_inference](/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)     | use some tweaks and heuristics to infer schema in CSV format. If disabled, all fields will be inferred as Strings. | `true`  |                                                                                                                                                                                              |
| [input_format_csv_arrays_as_nested_csv](/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                   | when reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string.      | `false` |                                                                                                                                                                                              |
| [output_format_csv_crlf_end_of_line](/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                         | if it is set to true, end of line in CSV output format will be `\r\n` instead of `\n`.                             | `false` |                                                                                                                                                                                              |
| [input_format_csv_skip_first_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                           | skip the specified number of lines at the beginning of data.                                                       | `0`     |                                                                                                                                                                                              |
| [input_format_csv_detect_header](/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                 | automatically detect header with names and types in CSV format.                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_skip_trailing_empty_lines](/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                         | skip trailing empty lines at the end of data.                                                                      | `false` |                                                                                                                                                                                              |
| [input_format_csv_trim_whitespaces](/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                           | trim spaces and tabs in non-quoted CSV strings.                                                                    | `true`  |                                                                                                                                                                                              |
| [input_format_csv_allow_whitespace_or_tab_as_delimiter](/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter)   | Allow to use whitespace or tab as field delimiter in CSV strings.                                                  | `false` |                                                                                                                                                                                              |
| [input_format_csv_allow_variable_number_of_columns](/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)           | allow variable number of columns in CSV format, ignore extra columns and use default values on missing columns.    | `false` |                                                                                                                                                                                              |
| [input_format_csv_use_default_on_bad_values](/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                         | Allow to set default value to column when CSV field deserialization failed on bad value.                           | `false` |                                                                                                                                                                                              |
| [input_format_csv_try_infer_numbers_from_strings](/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)               | Try to infer numbers from string fields while schema inference.                                                    | `false` |                                                                                                                                                                                              |