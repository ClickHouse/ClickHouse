---
title : RowBinaryWithNames
slug : /en/interfaces/formats/RowBinaryWithNames
keywords : [RowBinaryWithNames]
---

## Description

Similar to [RowBinary](/docs/en/interfaces/formats/RowBinary), but with added header:

- [LEB128](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N)
- N `String`s specifying column names

:::note
If setting [input_format_with_names_use_header](/docs/en/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
:::

## Example Usage

## Format Settings