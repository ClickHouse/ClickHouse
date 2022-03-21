---
toc_priority: 125
---

# groupBitAnd {#groupbitand}

对于数字序列按位应用 `AND` 。

**语法**

``` sql
groupBitAnd(expr)
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
SELECT groupBitAnd(num) FROM t
```

`num` 是包含测试数据的列。

结果:

``` text
binary     decimal
00000100 = 4
```
