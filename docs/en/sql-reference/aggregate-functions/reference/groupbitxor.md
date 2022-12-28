---
sidebar_position: 127
---

# groupBitXor

Applies bitwise `XOR` for series of numbers.

``` sql
groupBitXor(expr)
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
SELECT groupBitXor(num) FROM t
```

Where `num` is the column with the test data.

Result:

``` text
binary     decimal
01101000 = 104
```
