---
sidebar_position: 125
---

# groupBitAnd

Applies bitwise `AND` for series of numbers.

``` sql
groupBitAnd(expr)
```

**Arguments**

`expr` – An expression that results in `UInt*` type.

**Return value**

Value of the `UInt*` type.

**Example**

Test data:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Query:

``` sql
SELECT groupBitAnd(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
00000100 = 4
```
