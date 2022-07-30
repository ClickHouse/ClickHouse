---
sidebar_position: 126
---

# groupBitOr

Applies bitwise `OR` for series of numbers.

``` sql
groupBitOr(expr)
```

**Arguments**

`expr` – An expression that results in `UInt*` type.

**Returned value**

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
SELECT groupBitOr(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
01111101 = 125
```
