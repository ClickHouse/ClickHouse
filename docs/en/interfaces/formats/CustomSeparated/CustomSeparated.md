---
title : CustomSeparated
slug : /en/interfaces/formats/CustomSeparated
keywords : [CustomSeparated]
---

## Description

Similar to [Template](/docs/en/interfaces/formats/Template), but it prints or reads all names and types of columns and uses escaping rule from [format_custom_escaping_rule](/docs/en/operations/settings/settings-formats.md/#format_custom_escaping_rule) setting and delimiters from the following settings:
- [format_custom_field_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_field_delimiter)
- [format_custom_row_before_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
- [format_custom_row_after_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
- [format_custom_row_between_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
- [format_custom_result_before_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
- [format_custom_result_after_delimiter](/docs/en/operations/settings/settings-formats.md/#format_custom_result_after_delimiter) 

note:::
It does not use escaping rules settings and delimiters from format strings.
:::

There is also the `CustomSeparatedIgnoreSpaces` format, which is similar to [TemplateIgnoreSpaces](/docs/en/interfaces/formats/TemplateIgnoreSpaces).

## Example Usage

## Format Settings

Additional settings:
- [input_format_custom_detect_header](/docs/en/operations/settings/settings-formats.md/#input_format_custom_detect_header) - enables automatic detection of header with names and types if any. Default value - `true`.
- [input_format_custom_skip_trailing_empty_lines](/docs/en/operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines) - skip trailing empty lines at the end of file . Default value - `false`.
- [input_format_custom_allow_variable_number_of_columns](/docs/en/operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) - allow variable number of columns in CustomSeparated format, ignore extra columns and use default values on missing columns. Default value - `false`.