---
title : CSVWithNamesAndTypes
slug : /en/interfaces/formats/CSVWithNamesAndTypes
keywords : [CSVWithNamesAndTypes]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

Also prints two header rows with column names and types, similar to [TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes).

## Example Usage

## Format Settings

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

:::note
If setting [input_format_with_types_use_header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::