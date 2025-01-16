---
title : CustomSeparatedWithNames
slug : /en/interfaces/formats/CustomSeparatedWithNames
keywords : [CustomSeparatedWithNames]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

Also prints the header row with column names, similar to [TabSeparatedWithNames](../TabSeparated/TabSeparatedWithNames.md).

## Example Usage

## Format Settings

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns from the table by their names, 
columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::