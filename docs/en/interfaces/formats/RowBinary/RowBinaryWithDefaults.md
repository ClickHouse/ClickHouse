---
title : RowBinaryWithDefaults
slug : /en/interfaces/formats/RowBinaryWithDefaults
keywords : [RowBinaryWithDefaults]
---

## Description

Similar to [RowBinary](/docs/en/interfaces/formats/RowBinary), but with an extra byte before each column that indicates if default value should be used.

## Example Usage

Examples:

```sql
:) select * from format('RowBinaryWithDefaults', 'x UInt32 default 42, y UInt32', x'010001000000')

┌──x─┬─y─┐
│ 42 │ 1 │
└────┴───┘
```

For column `x` there is only one byte `01` that indicates that default value should be used and no other data after this byte is provided.
For column `y` data starts with byte `00` that indicates that column has actual value that should be read from the subsequent data `01000000`.

## Format Settings


