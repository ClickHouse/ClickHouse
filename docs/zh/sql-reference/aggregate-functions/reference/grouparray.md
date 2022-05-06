---
sidebar_position: 110
---

# groupArray {#agg_function-grouparray}

**语法**
``` sql
groupArray(x)
或
groupArray(max_size)(x)
```

创建参数值的数组。
值可以按任何（不确定）顺序添加到数组中。

第二个版本（带有 `max_size` 参数）将结果数组的大小限制为 `max_size` 个元素。
例如, `groupArray (1) (x)` 相当于 `[any (x)]` 。

在某些情况下，您仍然可以依赖执行顺序。这适用于SELECT(查询)来自使用了 `ORDER BY` 子查询的情况。
