---
title : TabSeparatedWithNames
slug : /en/interfaces/formats/TabSeparatedWithNames
keywords : [TabSeparatedWithNames]
input_format: true
output_format: true
alias: ['TSVWithNames']
---

| Input | Output | Alias                          |
|-------|--------|--------------------------------|
| 	✔    | 	✔     | `TSVWithNames`, `RawWithNames` |

## Description

Differs from the [`TabSeparated`](./TabSeparated.md) format in that the column names are written in the first row.

During parsing, the first row is expected to contain the column names. You can use column names to determine their position and to check their correctness.

:::note
If setting [`input_format_with_names_use_header`](../../../operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from the input data will be mapped to the columns of the table by their names, columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::

## Example Usage

## Format Settings