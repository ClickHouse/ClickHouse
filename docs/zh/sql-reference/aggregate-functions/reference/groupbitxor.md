---
sidebar_position: 127
---

# groupBitXor {#groupbitxor}

对于数字序列按位应用 `XOR` 。

**语法**

``` sql
groupBitXor(expr)
```

**参数**

`expr` – 结果为 `UInt*` 类型的表达式。

**返回值**

`UInt*` 类型的值。

**示例**

测试数据:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

查询:

``` sql
SELECT groupBitXor(num) FROM t
```

`num` 是包含测试数据的列。

结果:

``` text
binary     decimal
01101000 = 104
```
