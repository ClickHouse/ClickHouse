---
alias: []
description: 'Documentation for the RowBinaryWithDefaults format'
input_format: true
keywords: ['RowBinaryWithDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithDefaults
title: 'RowBinaryWithDefaults'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to the [`RowBinary`](./RowBinary.md) format, but with an extra byte before each column that indicates if the default value should be used.

## Example Usage {#example-usage}

Examples:

```sql title="Query"
SELECT * FROM FORMAT('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')
```
```response title="Response"
┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

- For column `x` there is only one byte `01` that indicates that default value should be used and no other data after this byte is provided.
- For column `y` data starts with byte `00` that indicates that column has actual value that should be read from the subsequent data `01000000`.

## Format Settings {#format-settings}

<RowBinaryFormatSettings/>


