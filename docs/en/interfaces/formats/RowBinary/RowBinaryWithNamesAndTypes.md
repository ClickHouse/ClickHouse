---
alias: []
description: 'Documentation for the RowBinaryWithNamesAndTypes format'
input_format: true
keywords: ['RowBinaryWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/RowBinaryWithNamesAndTypes
title: 'RowBinaryWithNamesAndTypes'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to the [RowBinary](./RowBinary.md) format, but with added header:

- [`LEB128`](https://en.wikipedia.org/wiki/LEB128)-encoded number of columns (N).
- N `String`s specifying column names.
- N `String`s specifying column types.

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<RowBinaryFormatSettings/>

:::note
If setting [`input_format_with_names_use_header`](/operations/settings/settings-formats.md/#input_format_with_names_use_header) is set to 1,
the columns from input data will be mapped to the columns from the table by their names, columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
Otherwise, the first row will be skipped.
If setting [`input_format_with_types_use_header`](/operations/settings/settings-formats.md/#input_format_with_types_use_header) is set to `1`,
the types from input data will be compared with the types of the corresponding columns from the table. Otherwise, the second row will be skipped.
:::