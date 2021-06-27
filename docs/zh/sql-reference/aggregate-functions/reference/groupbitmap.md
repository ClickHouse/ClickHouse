---
toc_priority: 128
---

# groupBitmap {#groupbitmap}

从无符号整数列进行位图或聚合计算，返回 `UInt64` 类型的基数，如果添加后缀 `State` ，则返回[位图对象](../../../sql-reference/functions/bitmap-functions.md)。

**语法**

``` sql
groupBitmap(expr)
```

**参数**

`expr` –  结果为 `UInt*` 类型的表达式。

**返回值**

`UInt64` 类型的值。

**示例**

测试数据:

``` text
UserID
1
1
2
3
```

查询:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

结果:

``` text
num
3
```
