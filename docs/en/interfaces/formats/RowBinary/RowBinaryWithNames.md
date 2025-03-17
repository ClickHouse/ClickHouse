---
title : RowBinaryWithNames
slug: /interfaces/formats/RowBinaryWithNames
keywords : [RowBinaryWithNames]
input_format: true
output_format: true
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

Similar to the [`RowBinary`](./RowBinary.md) format, but with added header:

- [`LEB128`](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N).
- N `String`s specifying column names.

## Example Usage

## Format Settings

<RowBinaryFormatSettings/>

:::note
- If setting [`input_format_with_names_use_header`](/docs/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to `1`,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped. 
- If setting [`input_format_skip_unknown_fields`](/docs/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.
Otherwise, the first row will be skipped.
:::