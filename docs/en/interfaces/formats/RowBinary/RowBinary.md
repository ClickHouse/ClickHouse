---
alias: []
description: 'Documentation for the RowBinary format'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `RowBinary` format parses data by row in binary format. 
Rows and values are listed consecutively, without separators. 
Because data is in the binary format the delimiter after `FORMAT RowBinary` is strictly specified as follows: 

- Any number of whitespaces:
  - `' '` (space - code `0x20`)
  - `'\t'` (tab - code `0x09`)
  - `'\f'` (form feed - code `0x0C`) 
- Followed by exactly one new line sequence:
  - Windows style `"\r\n"` 
  - or Unix style `'\n'`
- Immediately followed by binary data.

:::note
This format is less efficient than the [Native](../Native.md) format since it is row-based.
:::

For the following data types it is important to note that:

- [Integers](../../../sql-reference/data-types/int-uint.md) use fixed-length little-endian representation. For example, `UInt64` uses 8 bytes.
- [DateTime](../../../sql-reference/data-types/datetime.md) is represented as `UInt32` containing the Unix timestamp as the value.
- [Date](../../../sql-reference/data-types/date.md) is represented as a UInt16 object that contains the number of days since `1970-01-01` as the value.
- [String](../../../sql-reference/data-types/string.md) is represented as a variable-width integer (varint) (unsigned [`LEB128`](https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.
- [FixedString](../../../sql-reference/data-types/fixedstring.md) is represented simply as a sequence of bytes.
- [Arrays](../../../sql-reference/data-types/array.md) are represented as a variable-width integer (varint) (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128)), followed by successive elements of the array.

For [NULL](/sql-reference/syntax#null) support, an additional byte containing `1` or `0` is added before each [Nullable](/sql-reference/data-types/nullable.md) value. 
- If `1`, then the value is `NULL` and this byte is interpreted as a separate value. 
- If `0`, the value after the byte is not `NULL`.

For a comparison of the `RowBinary` format and the `RawBlob` format see: [Raw Formats Comparison](../RawBLOB.md/#raw-formats-comparison)

## Example Usage {#example-usage}

## Format Settings {#format-settings}

<RowBinaryFormatSettings/>