---
title : Values
slug : /en/interfaces/formats/Values
keywords : [Values]
---

## Description

Prints every row in brackets. Rows are separated by commas. There is no comma after the last row. 
The values inside the brackets are also comma-separated. Numbers are output in a decimal format without quotes. 
Arrays are output in square brackets. Strings, dates, and dates with times are output in quotes. Escaping rules and parsing are similar to the [TabSeparated](/docs/en/interfaces/formats/TabSeparated) format. 
During formatting, extra spaces aren’t inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). 
[NULL](/docs/en/sql-reference/syntax.md) is represented as `NULL`.
The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.
This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

## Example Usage

## Format Settings

- [input_format_values_interpret_expressions](/docs/en/operations/settings/settings-formats.md/#input_format_values_interpret_expressions) - if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression. Default value - `true`.
- [input_format_values_deduce_templates_of_expressions](/docs/en/operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) -if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows. Default value - `true`.
- [input_format_values_accurate_types_of_literals](/docs/en/operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals) - when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues. Default value - `true`.

## Values {#data-format-values}

