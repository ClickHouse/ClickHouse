---
alias: []
description: 'Documentation for the CustomSeparated format'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to [Template](../Template/Template.md), but it prints or reads all names and types of columns and uses escaping rule from [format_custom_escaping_rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule) setting and delimiters from the following settings:

- [format_custom_field_delimiter](/operations/settings/settings-formats.md/#format_custom_field_delimiter)
- [format_custom_row_before_delimiter](/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
- [format_custom_row_after_delimiter](/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
- [format_custom_row_between_delimiter](/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
- [format_custom_result_before_delimiter](/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
- [format_custom_result_after_delimiter](/operations/settings/settings-formats.md/#format_custom_result_after_delimiter) 

note:::
It does not use escaping rules settings and delimiters from format strings.
:::

There is also the [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md) format, which is similar to [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md).

## Example Usage {#example-usage}

## Format Settings {#format-settings}

Additional settings:

| Setting                                                                                                                                                        | Description                                                                                                                 | Default |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------|
| [input_format_custom_detect_header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                       | enables automatic detection of header with names and types if any.                                                          | `true`  |
| [input_format_custom_skip_trailing_empty_lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)               | skip trailing empty lines at the end of file.                                                                              | `false` |
| [input_format_custom_allow_variable_number_of_columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | allow variable number of columns in CustomSeparated format, ignore extra columns and use default values for missing columns. | `false` |