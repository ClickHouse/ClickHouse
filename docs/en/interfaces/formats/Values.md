---
title : Values
slug : /en/interfaces/formats/Values
keywords : [Values]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

The `Values` format prints every row in brackets. 

- Rows are separated by commas without a comma after the last row. 
- The values inside the brackets are also comma-separated. 
- Numbers are output in a decimal format without quotes. 
- Arrays are output in square brackets. 
- Strings, dates, and dates with times are output in quotes. 
- Escaping rules and parsing are similar to the [TabSeparated](TabSeparated/TabSeparated.md) format.

During formatting, extra spaces aren’t inserted, but during parsing, they are allowed and skipped (except for spaces inside array values, which are not allowed). 
[`NULL`](/docs/en/sql-reference/syntax.md) is represented as `NULL`.

The minimum set of characters that you need to escape when passing data in the `Values` format: 
- single quotes
- backslashes

This is the format that is used in `INSERT INTO t VALUES ...`, but you can also use it for formatting query results.

## Example Usage

## Format Settings

| Setting                                                                                                                                                     | Description                                                                                                                                                                                   | Default |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.                                                                               | `true`  |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows. | `true`  |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.                                                       | `true`  |

