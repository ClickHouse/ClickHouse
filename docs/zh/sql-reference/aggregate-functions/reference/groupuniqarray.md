---
sidebar_position: 111
---

# groupUniqArray {#groupuniqarray}

**语法**

``` sql
groupUniqArray(x)
或
groupUniqArray(max_size)(x)
```

从不同的参数值创建一个数组。 内存消耗和 [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md) 函数是一样的。

第二个版本（带有 `max_size` 参数）将结果数组的大小限制为 `max_size` 个元素。
例如, `groupUniqArray(1)(x)` 相当于 `[any(x)]`.
